"""Tests for the CSV pipeline.

Covers the current v2 API:
    ingest -> validate -> transform -> aggregate -> export -> notify -> archive
"""

import os
import sys
import csv
import json
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pipeline
from pipeline import (
    stage_ingest,
    stage_validate,
    stage_transform,
    stage_aggregate,
    stage_export,
    stage_notify,
    run_pipeline,
)


@pytest.fixture
def sample_csv():
    """Create a temporary sample CSV."""
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='')
    writer = csv.writer(tmp)
    writer.writerow(['name', 'email', 'age', 'department'])
    writer.writerow([' Alice Johnson ', 'alice@example.com', '32', 'Engineering'])
    writer.writerow(['Bob Smith', 'bob@example.com', '45', ' Marketing '])
    writer.writerow(['', 'charlie@example.com', '28', 'Engineering'])
    tmp.close()
    yield tmp.name
    if os.path.exists(tmp.name):
        os.unlink(tmp.name)


class TestStageIngest:
    def test_reads_valid_csv(self, sample_csv):
        df = stage_ingest(sample_csv)
        assert df is not None
        assert len(df) == 3

    def test_returns_none_for_missing_file(self):
        result = stage_ingest('nonexistent.csv')
        assert result is None

    def test_returns_none_for_unreadable_file(self, tmp_path):
        # A directory is not a CSV; ingest must fail soft (return None).
        result = stage_ingest(str(tmp_path))
        assert result is None

    def test_columns_are_correct(self, sample_csv):
        df = stage_ingest(sample_csv)
        expected = ['name', 'email', 'age', 'department']
        assert list(df.columns) == expected


class TestStageValidate:
    def test_detects_missing_values(self, sample_csv):
        df = stage_ingest(sample_csv)
        issues = stage_validate(df)
        assert isinstance(issues, list)
        # The row with an empty name leaves one null in the 'name' column.
        assert any(i['column'] == 'name' and i['nulls'] == 1 for i in issues)

    def test_clean_frame_has_no_issues(self):
        import pandas as pd
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        assert stage_validate(df) == []


class TestStageTransform:
    def test_trim_whitespace(self, sample_csv):
        df = stage_ingest(sample_csv)
        result = stage_transform(df, sample_csv)
        assert result is not None
        assert result.iloc[0]['name'] == 'Alice Johnson'

    def test_blank_names_do_not_crash_and_are_filled(self, sample_csv):
        df = stage_ingest(sample_csv)
        result = stage_transform(df, sample_csv)
        # A completely empty row would be dropped; a partial row is filled.
        assert not result.isna().any().any()


class TestStageAggregate:
    def test_report_shape(self, sample_csv):
        df = stage_transform(stage_ingest(sample_csv), sample_csv)
        report = stage_aggregate(df)
        assert report['row_count'] == len(df)
        assert report['column_count'] == len(df.columns)
        assert set(report['columns']) == {'name', 'email', 'age', 'department'}
        assert 'age' in report['numeric_summary']
        assert report['numeric_summary']['age']['max'] == 45


class TestStageExport:
    def test_exports_to_json(self, sample_csv):
        df = stage_ingest(sample_csv)
        df = stage_transform(df, sample_csv)
        issues = stage_validate(df)
        report = stage_aggregate(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = stage_export(df, tmpdir, sample_csv, issues, report, 3)
            assert result is not None
            assert result.endswith('.json')
            assert os.path.exists(result)
            with open(result, encoding='utf-8') as fh:
                payload = json.load(fh)
            assert payload['metadata']['original_rows'] == 3
            assert payload['metadata']['final_rows'] == len(df)
            assert len(payload['data']) == len(df)

    def test_export_status_warns_on_issues(self, sample_csv):
        df = stage_transform(stage_ingest(sample_csv), sample_csv)
        issues = stage_validate(stage_ingest(sample_csv))
        report = stage_aggregate(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = stage_export(df, tmpdir, sample_csv, issues, report, 3)
            with open(result, encoding='utf-8') as fh:
                payload = json.load(fh)
            assert payload['metadata']['status'] == 'warning'


class TestStageNotify:
    def test_no_webhook_is_a_noop(self, monkeypatch):
        monkeypatch.setattr(pipeline, 'WEBHOOK_URL', '')
        assert stage_notify('x.csv', 'success', {}, [], {}) is False


class TestRunPipeline:
    def test_full_pipeline_runs_and_archives(self, sample_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, 'output')
            archive = os.path.join(tmpdir, 'archive')
            result = run_pipeline(sample_csv, output, archive)
            assert result is True
            # Source should have moved into the archive, and an export written.
            assert os.path.exists(os.path.join(archive, os.path.basename(sample_csv)))
            assert not os.path.exists(sample_csv)
            assert any(f.endswith('.json') for f in os.listdir(output))

    def test_pipeline_on_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, 'output')
            archive = os.path.join(tmpdir, 'archive')
            result = run_pipeline('nonexistent.csv', output, archive)
            assert result is False
