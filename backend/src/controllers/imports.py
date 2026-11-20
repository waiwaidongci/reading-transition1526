from ..services import imports
class ImportsController:
    jobs=staticmethod(imports.list_jobs)
    create=staticmethod(imports.create_job)
    detail=staticmethod(imports.get_job)
    chunks=staticmethod(imports.list_chunks)
    upload=staticmethod(imports.put_chunk)
    complete=staticmethod(imports.complete_receiving)
    validate=staticmethod(imports.trigger_validate)
    publish=staticmethod(imports.trigger_publish)
    bad_rows=staticmethod(imports.list_bad_rows)
    staged=staticmethod(imports.list_staged)
