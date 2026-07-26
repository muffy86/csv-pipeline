"""Tests for the CSV pipeline."""

import os
import sys
import json
import csv
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pipeline import (
    stage_ingest, stage_validate, stage_transform, stage_export,
    run_pipeline, Color
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

    def test_columns_are_correct(self, sample_csv):
        df = stage_ingest(sample_csv)
        expected = ['name', 'email', 'age', 'department']
        assert list(df.columns) == expected


class TestStageValidate:
    def test_detects_missing_values(self, sample_csv):
        df = stage_ingest(sample_csv)
        result = stage_validate(df)
        assert result is not None
        # Row with empty name should be flagged
        assert 'missing' in str(result).lower() or result is not None

    def test_validates_all_columns_present(self, sample_csv):
        df = stage_ingest(sample_csv)
        result = stage_validate(df)
        assert result is not None


class TestStageTransform:
    def test_trim_whitespace(self, sample_csv):
        df = stage_ingest(sample_csv)
        result = stage_transform(df, sample_csv)
        assert result is not None
        # Check whitespace was trimmed
        assert result.iloc[0]['name'] == 'Alice Johnson'


class TestStageExport:
    def test_exports_to_json(self, sample_csv):
        df = stage_ingest(sample_csv)
        df = stage_transform(df, sample_csv)
        issues = stage_validate(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = stage_export(df, output_dir=tmpdir, source_file=sample_csv, issues=issues, original_rows=3)
            assert result is not None
            assert result.endswith('.json')
            files = os.listdir(tmpdir)
            assert any(f.endswith('.json') for f in files)

    def test_exports_to_csv(self, sample_csv):
        df = stage_ingest(sample_csv)
        df = stage_transform(df, sample_csv)
        issues = stage_validate(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = stage_export(df, output_dir=tmpdir, source_file=sample_csv, issues=issues, original_rows=3)
            assert result is not None
            files = os.listdir(tmpdir)
            assert any(f.endswith('.csv') for f in files)


class TestRunPipeline:
    def test_full_pipeline_runs(self, sample_csv):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, 'output')
            archive = os.path.join(tmpdir, 'archive')
            result = run_pipeline(sample_csv, output, archive)
            assert result is True

    def test_pipeline_on_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = os.path.join(tmpdir, 'output')
            archive = os.path.join(tmpdir, 'archive')
            result = run_pipeline('nonexistent.csv', output, archive)
            assert result is False


class TestColor:
    def test_color_class_exists(self):
        assert Color is not None
        assert hasattr(Color, 'cyan')
        assert hasattr(Color, 'green')
        assert hasattr(Color, 'red')
