import {api,upload} from './client.js';
export const importsApi={
  list:()=>api('/imports'),
  create:data=>api('/imports',{method:'POST',body:JSON.stringify(data)}),
  get:id=>api('/imports/'+id),
  chunks:id=>api('/imports/'+id+'/chunks'),
  putChunk:(id,index,offset,bytes)=>upload('/imports/'+id+'/chunks/'+index+'?offset='+offset,bytes),
  complete:(id,checksum='')=>api('/imports/'+id+'/complete',{method:'POST',body:JSON.stringify({checksum_sha256:checksum})}),
  validate:id=>api('/imports/'+id+'/validate',{method:'POST'}),
  publish:id=>api('/imports/'+id+'/publish',{method:'POST'}),
  badRows:(id,limit=200,offset=0)=>api('/imports/'+id+'/bad-rows?limit='+limit+'&offset='+offset),
  staged:(id,limit=100,offset=0)=>api('/imports/'+id+'/staged?limit='+limit+'&offset='+offset)
};
// 浏览器侧分片大小：4MB，百万行文件按偏移顺序/断点续传
export const CHUNK_BYTES=4*1024*1024;
