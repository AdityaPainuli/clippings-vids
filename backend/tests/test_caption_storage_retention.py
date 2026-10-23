
import sys
import types
from unittest.mock import Mock

import pytest


@pytest.fixture
def storage_module(monkeypatch):
    """Import caption storage without requiring real Supabase credentials."""
    client_module = types.ModuleType("supabase_client")
    client_module.supabase = Mock()
    client_module.SUPABASE_URL = "https://example.invalid"
    client_module.SUPABASE_SERVICE_KEY = "test-key"

    monkeypatch.setitem(sys.modules, "supabase_client", client_module)

    # Import after installing the stub, so no real client is initialized.
    sys.modules.pop("captions.storage", None)
    from captions import storage

    monkeypatch.setattr(storage, "_delete_storage_paths", Mock())
    yield storage
    sys.modules.pop("captions.storage", None)


def test_expired_transcribe_preserves_source_used_by_live_render(
    storage_module, monkeypatch, tmp_path
):
    storage = storage_module
    shared_source = "user-1/sources/upload-1/video.mp4"
    expired_output = "user-1/outputs/transcribe-1/transcript.srt"

    expired_jobs = [
        {
            "id": "transcribe-1",
            "source_path": shared_source,
            "output_path": expired_output,
        }
    ]
    live_jobs = [{"source_path": shared_source}]

    expired_query = Mock()
    expired_query.select.return_value = expired_query
    expired_query.lt.return_value = expired_query
    expired_query.execute.return_value = Mock(data=expired_jobs)

    active_query = Mock()
    active_query.select.return_value = active_query
    active_query.gte.return_value = active_query
    active_query.execute.return_value = Mock(data=live_jobs)

    delete_query = Mock()
    delete_query.in_.return_value = delete_query
    delete_query.execute.return_value = Mock(data=[])

    delete_builder = Mock()
    delete_builder.delete.return_value = delete_query

    table = Mock()
    table.side_effect = [expired_query, active_query, delete_builder]
    storage.supabase.table = table

    monkeypatch.setattr(storage.os.path, "isdir", lambda _: False)

    deleted_count = storage.delete_expired(local_dir=str(tmp_path))

    assert deleted_count == 1
    storage._delete_storage_paths.assert_called_once_with([expired_output])
    delete_query.in_.assert_called_once_with("id", ["transcribe-1"])


def test_expired_source_is_deleted_when_no_live_job_references_it(
    storage_module, monkeypatch, tmp_path
):
    storage = storage_module
    expired_source = "user-1/sources/upload-2/video.mp4"

    expired_jobs = [
        {
            "id": "transcribe-2",
            "source_path": expired_source,
            "output_path": None,
        }
    ]

    expired_query = Mock()
    expired_query.select.return_value = expired_query
    expired_query.lt.return_value = expired_query
    expired_query.execute.return_value = Mock(data=expired_jobs)

    active_query = Mock()
    active_query.select.return_value = active_query
    active_query.gte.return_value = active_query
    active_query.execute.return_value = Mock(data=[])

    delete_query = Mock()
    delete_query.in_.return_value = delete_query
    delete_query.execute.return_value = Mock(data=[])

    delete_builder = Mock()
    delete_builder.delete.return_value = delete_query

    table = Mock()
    table.side_effect = [expired_query, active_query, delete_builder]
    storage.supabase.table = table

    monkeypatch.setattr(storage.os.path, "isdir", lambda _: False)

    deleted_count = storage.delete_expired(local_dir=str(tmp_path))

    assert deleted_count == 1
    storage._delete_storage_paths.assert_called_once_with([expired_source])
