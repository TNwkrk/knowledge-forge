# Knowledge Forge — Extraction Schemas

This document defines the canonical extraction record types introduced in
`F-1`. These Pydantic models are the source of truth for schema-bound LLM
extraction prompts and downstream validation.

## Shared provenance contract

Every extraction record includes:

- `source_doc_id` — canonical manual identifier
- `source_page_range` — inclusive page range with `start_page` and `end_page`
- `source_heading` — source section heading used for extraction
- `parser_version` — parser build/version that produced the section
- `extraction_version` — extraction schema or prompt version
- `confidence` — normalized extraction confidence between `0.0` and `1.0`
- `bucket_context` — one or more bucket assignments that scope comparison work

## Record types

### `procedure`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 18, "end_page": 20},
  "source_heading": "Startup Procedure",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.94,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "title": "Start the controller",
  "steps": [
    {
      "source_doc_id": "honeywell-dc1000-service-manual-rev3",
      "source_page_range": {"start_page": 18, "end_page": 18},
      "source_heading": "Startup Procedure",
      "parser_version": "docling-1.2.0",
      "extraction_version": "f1",
      "confidence": 0.93,
      "bucket_context": [
        {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
      ],
      "step_number": 1,
      "instruction": "Verify the discharge valve is open.",
      "note": "Use local lockout procedure before inspection.",
      "caution": "Do not energize the motor with the casing dry.",
      "figure_ref": "Fig. 3-2"
    }
  ],
  "applicability": {
    "source_doc_id": "honeywell-dc1000-service-manual-rev3",
    "source_page_range": {"start_page": 18, "end_page": 20},
    "source_heading": "Startup Procedure",
    "parser_version": "docling-1.2.0",
    "extraction_version": "f1",
    "confidence": 0.91,
    "bucket_context": [
      {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
    ],
    "manufacturer": "Honeywell",
    "family": "DC1000",
    "models": ["DC1000", "DC1100"],
    "serial_range": "SN1000-SN2999",
    "revision": "Rev 3"
  },
  "warnings": [],
  "tools_required": ["multimeter", "torque wrench"]
}
```

### `procedure_step`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 18, "end_page": 18},
  "source_heading": "Startup Procedure",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.93,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "step_number": 2,
  "instruction": "Apply control power and wait for the ready indicator.",
  "note": "Expected warm-up time is under 30 seconds.",
  "caution": null,
  "figure_ref": "Fig. 3-3"
}
```

### `warning`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 9, "end_page": 9},
  "source_heading": "Safety",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.98,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "severity": "danger",
  "text": "Disconnect mains power before opening the control cabinet.",
  "context": "Electrical service",
  "applicability": null
}
```

### `spec_value`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 42, "end_page": 42},
  "source_heading": "Electrical Specifications",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.95,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/model-applicability", "dimension": "model_applicability", "value": "DC1000"}
  ],
  "parameter": "Supply voltage",
  "value": "24",
  "unit": "VDC",
  "conditions": "Nominal input",
  "applicability": null
}
```

### `alarm_definition`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 56, "end_page": 57},
  "source_heading": "Alarm Codes",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.96,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "code": "AL-14",
  "description": "Low process pressure",
  "cause": "Measured pressure stayed below threshold for 10 seconds.",
  "remedy": "Inspect suction line and confirm transducer calibration.",
  "severity": "warning"
}
```

### `troubleshooting_entry`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 61, "end_page": 61},
  "source_heading": "Troubleshooting",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.92,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "symptom": "Display remains blank after startup.",
  "possible_causes": ["No control power", "Blown input fuse"],
  "remedies": ["Check incoming supply", "Replace fuse F1 with like-rated fuse"]
}
```

### `part_reference`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 74, "end_page": 74},
  "source_heading": "Replacement Parts",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.97,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "part_number": "320044-001",
  "description": "Input fuse, 2A slow-blow",
  "quantity": 1,
  "applicability": null
}
```

### `applicability`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 3, "end_page": 3},
  "source_heading": "Document Scope",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.9,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
  ],
  "manufacturer": "Honeywell",
  "family": "DC1000",
  "models": ["DC1000", "DC1100"],
  "serial_range": "SN1000-SN2999",
  "revision": "Rev 3"
}
```

### `revision_note`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 2, "end_page": 2},
  "source_heading": "Revision History",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.89,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/revision-authority", "dimension": "revision", "value": "Rev 3"}
  ],
  "revision_id": "Rev 3",
  "date": "2025-01-12",
  "changes": ["Updated wiring diagram", "Added AL-14 troubleshooting guidance"],
  "supersedes": "Rev 2"
}
```

### `supersession_candidate`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 2, "end_page": 2},
  "source_heading": "Revision History",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.84,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/revision-authority", "dimension": "revision", "value": "Rev 3"}
  ],
  "superseding_record_id": "dc1000--revision-note--003",
  "superseded_record_id": "dc1000--revision-note--002",
  "rationale": "Rev 3 explicitly replaces the previous wiring instructions.",
  "precedence_basis": "newer revision note"
}
```

### `contradiction_candidate`

```json
{
  "source_doc_id": "honeywell-dc1000-service-manual-rev3",
  "source_page_range": {"start_page": 42, "end_page": 42},
  "source_heading": "Electrical Specifications",
  "parser_version": "docling-1.2.0",
  "extraction_version": "f1",
  "confidence": 0.81,
  "bucket_context": [
    {"bucket_id": "honeywell/dc1000/model-applicability", "dimension": "model_applicability", "value": "DC1000"}
  ],
  "record_ids": ["dc1000--spec--001", "dc1000--spec--014"],
  "conflicting_claim": "Nominal supply voltage differs between two authoritative sources.",
  "rationale": "One manual states 24 VDC and a later bulletin states 48 VDC.",
  "review_status": "unreviewed"
}
```

## Registry and schema export

The canonical registry lives in `knowledge_forge.extract.schemas`:

- `SCHEMA_REGISTRY` maps record type strings to Pydantic model classes
- `JSON_SCHEMA_REGISTRY` exports prompt-ready JSON Schema for every type
- `get_schema_model(record_type)` performs model lookup
- `get_json_schema(record_type)` returns the matching JSON Schema
