import json
from pathlib import Path
import tempfile
import unittest
from kb.speech_run_evidence import record_speech_run,sha256

class WP04SpeechRunEvidenceTests(unittest.TestCase):
    def test_index_and_query_evidence_are_source_owned(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            inp=root/'input.json';out=root/'index.json'
            inp.write_text('{"real_source_ref":"speech:immutable"}')
            out.write_text('{"schema_name":"knowledge_inspect.speech_lexical_index.s3.v1"}')
            d=record_speech_run(root=root/'runs',mode='index',
                started_at='2026-10-25T00:00:00Z',
                finished_at='2026-10-25T00:00:02Z',
                source_file=inp,output_file=out)
            manifest=json.loads((d/'manifest.json').read_text())
            record=json.loads((d/'run_record.json').read_text())
            proof=json.loads((d/'source_proof.json').read_text())
            self.assertEqual(manifest['manifest_version'],2)
            self.assertEqual(manifest['run_id'],record['run_id'])
            self.assertEqual(proof['manifest_sha256'],sha256(d/'manifest.json'))
            self.assertEqual(record['evidence']['derived_output_sha256'],sha256(out))
            self.assertEqual(record['evidence']['output_path'],str(out.resolve()))
            self.assertNotIn('immutable',json.dumps(manifest))
            self.assertEqual(proof['producer_id'],'producer.manual.knowledge-inspect-speech')
    def test_missing_actual_output_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            inp=root/'input.json';inp.write_text('{}')
            with self.assertRaises(ValueError):
                record_speech_run(root=root/'runs',mode='query',
                    started_at='2026-10-25T00:00:00Z',finished_at='2026-10-25T00:00:01Z',
                    source_file=inp,output_file=root/'absent.json')
