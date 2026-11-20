import {research} from '../stores/research.js';
import {session} from '../stores/session.js';
import {importsApi,CHUNK_BYTES} from '../api/imports.js';
import {sourceOptions} from '../components/common/business.js';
import {importStates} from '../constants/enums.js';
import {esc,localTime} from '../utils/format.js';
import {action,toast,bind} from '../hooks/actions.js';

const ui={jobs:[],selectedId:null,detail:null,chunks:[],bad:null,staged:[],upload:{active:false,done:0,total:0,fileName:''}};

async function sha256Hex(file){
  const buf=await file.arrayBuffer();
  const digest=await crypto.subtle.digest('SHA-256',buf);
  return [...new Uint8Array(digest)].map(b=>b.toString(16).padStart(2,'0')).join('');
}
function fmtBytes(n){if(n==null)return '—';if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(2)+' MB';}
function pct(done,total){return total?Math.min(100,Math.round(done/total*100)):0;}

async function loadDetail(id){
  if(!id){ui.detail=null;ui.chunks=[];ui.bad=null;ui.staged=[];return;}
  ui.selectedId=id;
  [ui.detail,ui.chunks]=await Promise.all([importsApi.get(id),importsApi.chunks(id)]);
  if(ui.detail.bad_rows>0)ui.bad=await importsApi.badRows(id);
  else ui.bad=null;
  if(['validated','publishing','published'].includes(ui.detail.state))ui.staged=await importsApi.staged(id);
  else ui.staged=[];
}

export async function importsPage(){
  ui.jobs=await importsApi.list();
  if(!ui.selectedId&&ui.jobs.length)await loadDetail(ui.jobs[0].id);
  else if(ui.selectedId)await loadDetail(ui.selectedId);
  const j=ui.detail;
  const writable=session.writable;
  return '<section class="page-heading"><p class="eyebrow">07 / RESUMABLE LOG IMPORT</p><h1>百万行日志，断点分批导入。</h1><p>按文件偏移续传，重复块不重复写入；合格样本整批校验后一次发布，研究只见整批或完全不见。</p></section>'
  +'<div class="split"><section><h2>导入作业</h2>'+jobTable()
  +(j?'<div class="import-detail"><h3>作业 #'+j.id+' · '+esc(j.filename)+'</h3>'+stagePanel(j)+'</div>':'<div class="empty">还没有导入作业。</div>')
  +(ui.bad?badPanel():'')+(ui.staged.length?stagedPanel():'')
  +'</section><aside class="entry-sheet">'+(writable?newJobForm():'<h2>阅读权限</h2><p>可查看全部导入作业、校验结果与隔离原因；导入操作由负责人或分析师完成。</p>')+'</aside></div>';
}

function jobTable(){
  if(!ui.jobs.length)return '<div class="empty"><strong>这里还没有导入作业。</strong><p>在右侧选择文集并选择日志文件，系统自动分片上传并支持断点续传。</p></div>';
  return '<div class="table-wrap"><table><thead><tr><th>#</th><th>文件</th><th>状态</th><th>接收</th><th>合格/坏行</th><th>更新时间</th></tr></thead><tbody>'
  +ui.jobs.map(x=>'<tr class="'+(x.id===ui.selectedId?'selected-row':'')+'" data-job="'+x.id+'" style="cursor:pointer"><td>'+x.id+'</td><td>'+esc(x.filename)+'</td><td><span class="status '+x.state+'">'+esc(importStates[x.state]||x.state)+'</span></td><td>'+x.received_chunks+'/'+x.total_chunks+' 片</td><td>'+x.valid_rows+' / '+x.bad_rows+'</td><td>'+esc(localTime(x.updated_at))+'</td></tr>').join('')
  +'</tbody></table></div>';
}

function stagePanel(j){
  const receiving=pct(j.received_bytes,j.total_size);
  const rows=[
    ['接收',j.received_chunks+'/'+j.total_chunks+' 片 · '+fmtBytes(j.received_bytes)+'/'+fmtBytes(j.total_size),receiving],
    ['校验',j.state==='receiving'||j.state==='received'?0:(j.state==='validating'?60:100),j.state==='validating'?60:(['validated','publishing','published','failed'].includes(j.state)?100:0)],
    ['发布',['published'].includes(j.state)?100:(j.state==='publishing'?60:0),['published'].includes(j.state)?100:(j.state==='publishing'?60:0)]
  ];
  let html='<div class="stage-bars">'+rows.map(([label,,value])=>'<div class="stage-row"><span>'+label+'</span><div class="bar"><i style="width:'+value+'%"></i></div><b>'+value+'%</b></div>').join('')+'</div>';
  html+='<dl class="metrics"><div><dt>总行数</dt><dd>'+j.total_rows+'</dd></div><div><dt>合格</dt><dd>'+j.valid_rows+'</dd></div><div><dt>坏行隔离</dt><dd>'+j.bad_rows+'</dd></div><div><dt>重复去重</dt><dd>'+j.duplicate_rows+'</dd></div><div><dt>冲突</dt><dd>'+j.conflict_rows+'</dd></div></dl>';
  if(j.file_sha256)html+='<p class="file-note">整文件 SHA256：<code>'+esc(j.file_sha256.slice(0,24))+'…</code></p>';
  if(j.error)html+='<div class="failure-note"><strong>失败原因：</strong>'+esc(j.error)+'</div>';
  if(ui.upload.active)html+='<div class="stage-row"><span>上传中 '+esc(ui.upload.fileName)+'</span><div class="bar"><i style="width:'+pct(ui.upload.done,ui.upload.total)+'%"></i></div><b>'+pct(ui.upload.done,ui.upload.total)+'%</b></div>';
  if(session.writable){
    if(j.state==='receiving')html+='<input type="file" id="resume-file" accept=".csv,.log,.txt" hidden><div class="import-actions"><button id="btn-resume">续传剩余分片</button><button id="btn-complete" class="primary">完成接收并校验</button></div>';
    if(j.state==='received'||j.state==='failed')html+='<div class="import-actions"><button id="btn-revalidate">重新校验</button></div>';
    if(j.state==='validated')html+='<div class="import-actions"><button id="btn-publish" class="primary">一次发布 '+j.valid_rows+' 条合格样本</button></div>';
  }
  return html;
}

function badPanel(){
  return '<details open class="bad-panel"><summary>坏行隔离区（'+ui.bad.total+' 行，保留原行与原因）</summary><div class="table-wrap"><table><thead><tr><th>行号</th><th>原始行</th><th>隔离原因</th></tr></thead><tbody>'
  +ui.bad.items.map(b=>'<tr><td>'+b.line_no+'</td><td><code>'+esc(b.raw_line||'（库内冲突，无文件原行）')+'</code></td><td>'+esc(b.reason)+'</td></tr>').join('')
  +'</tbody></table></div></details>';
}
function stagedPanel(){
  return '<details class="bad-panel"><summary>暂存样本预览（前 '+ui.staged.length+' 行，发布前对研究不可见）</summary><div class="table-wrap"><table><thead><tr><th>行号</th><th>会话</th><th>路径</th></tr></thead><tbody>'
  +ui.staged.map(s=>'<tr><td>'+s.line_no+'</td><td>'+esc(s.values.session)+'</td><td>'+esc(s.values.path).replace(/\n/g,'↵')+'</td></tr>').join('')
  +'</tbody></table></div></details>';
}

function newJobForm(){
  return '<h2>新建日志导入</h2><form id="import-form"><label>目标文集<select name="source_id">'+sourceOptions(research.sources,research.sourceId)+'</select></label>'
  +'<label>日志文件（CSV：session,path；支持引号内含换行）<input name="file" type="file" accept=".csv,.log,.txt" required></label>'
  +'<button type="submit" id="import-submit">创建并分片上传 →</button></form>'
  +'<p class="file-note">上传中断后可点“续传”，已接收分片自动跳过；同偏移异值将拒收并终止作业。</p>';
}

export function bindImports(refresh){
  bind('#import-form','submit',async event=>{
    event.preventDefault();
    const form=event.target;const file=form.file.files[0];
    if(!file)return;
    const sourceId=Number(form.source_id.value);
    const totalChunks=Math.ceil(file.size/CHUNK_BYTES)||1;
    const btn=form.querySelector('#import-submit');btn.disabled=true;
    try{
      const job=await importsApi.create({source_id:sourceId,filename:file.name,total_size:file.size,total_chunks:totalChunks});
      await loadDetail(job.id);await refresh();
      await uploadWithResume(job.id,file,refresh);
    }catch(e){toast(e.message,true)}finally{btn.disabled=false}
  });
  document.querySelectorAll('[data-job]').forEach(el=>el.addEventListener('click',async()=>{await loadDetail(Number(el.dataset.job));await refresh()}));
  action('#btn-complete',async()=>{
    let checksum='';
    if(_currentFile){try{checksum=await sha256Hex(_currentFile)}catch{checksum=''}}
    await importsApi.complete(ui.selectedId,checksum);toast('接收完成，已进入流式校验');await pollAndRefresh(refresh);
  });
  bind('#btn-resume','click',()=>document.getElementById('resume-file')?.click());
  bind('#resume-file','change',async e=>{
    const file=e.target.files[0];if(!file)return;
    try{await uploadWithResume(ui.selectedId,file,refresh)}catch(err){toast(err.message,true)}
  });
  action('#btn-revalidate',async()=>{await importsApi.validate(ui.selectedId);toast('已重新投递校验');await pollAndRefresh(refresh)});
  action('#btn-publish',async()=>{await importsApi.publish(ui.selectedId);toast('发布请求已提交，单事务写入');await pollAndRefresh(refresh)});
}

let _currentFile=null;

async function uploadWithResume(jobId,file,refresh){
  _currentFile=file;
  const serverChunks=new Set((await importsApi.chunks(jobId)).map(c=>c.chunk_index));
  const total=Math.ceil(file.size/CHUNK_BYTES)||1;
  ui.upload={active:true,done:serverChunks.size,total,fileName:file.name};
  for(let i=0;i<total;i++){
    if(serverChunks.has(i))continue;  // 断点续传：已落盘分片跳过，重复块不重复写
    const slice=file.slice(i*CHUNK_BYTES,Math.min(file.size,(i+1)*CHUNK_BYTES));
    const bytes=await slice.arrayBuffer();
    await importsApi.putChunk(jobId,i,i*CHUNK_BYTES,bytes);
    serverChunks.add(i);ui.upload.done=serverChunks.size;
    if(i%4===0)await refresh();
  }
  ui.upload.active=false;
  toast('分片全部接收，可完成接收并校验');
  await refresh();
}

async function pollAndRefresh(refresh){
  for(let k=0;k<40;k++){
    await new Promise(r=>setTimeout(r,1200));
    await loadDetail(ui.selectedId);await refresh();
    const s=ui.detail?.state;
    if(['validated','published','failed'].includes(s))break;
  }
}
