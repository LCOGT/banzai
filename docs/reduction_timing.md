# Reduction timing logs

BANZAI emits INFO records with message `Reduction timing`, `event=reduction_timing`,
a stable `operation`, an `outcome`, and a numeric `duration_s`. Durations use
`time.perf_counter()` and measure elapsed wall time, including any blocking I/O,
retries, or waits inside the measured call. They are not CPU time.
Existing pipeline log messages remain in place.

## Correlation and sample selection

When a frame exists, records include the existing `filename`, `request_num`,
`site`, `instrument`, `epoch`, `obstype`, and `filter` tags, plus:

- `input_filename`: the frame being reduced, kept distinct from
  `calibration_filename` and `output_filename` on their respective operations.
- `camera`, `process_id`, and the existing logger's `processName`.
- `image_shapes`: actual CCD array shapes in NumPy order (`[rows, columns]` for
  a 2-D extension), one entry per CCD extension. Header-only primaries are not
  counted. Reading shapes does not scan or copy arrays.
- Header values, when present: `configuration_mode` (`CONFMODE`),
  `observing_mode` (`OBSMODE`), `reduction_level` (`RLEVEL`),
  `smartstack_moluid` (`MOLUID`), `smartstack_stack_num` (`MOLFRNUM`), and
  `smartstack_frmtotal` (`FRMTOTAL`).

Use `configuration_mode` and `image_shapes` to compare full-frame and
`central30x30` data; no request IDs are hardcoded. Identity and shapes describe
the input to each operation. `input_open` captures them after construction,
and `reduction_total` retains that initial snapshot even after trimming or
updating `RLEVEL`. Calibration records describe the science frame being
calibrated; `calibration_filename` identifies the master separately.
Output operations capture the current reduced image shape.

If opening raises or returns `None`, only the supplied input filename and
process identity may be available. No extra file is opened to obtain logging
metadata. Malformed frame metadata sets `timing_metadata_error=true` and does
not stop the measured operation. These fields are structured logger tags;
the usual formatter also supplies the log timestamp. Retain collector-provided
host/pod metadata when joining logs from multiple worker containers.

## Exact duration boundaries

| `operation` | Measured work |
| --- | --- |
| `input_open` | One entire `frame_factory.open(...)` call from `run_pipeline_stages`, including existing file/cache/archive reads, FITS decoding/unpacking, conversion, data/mask/uncertainty memmap construction, instrument lookup, header initialization, and acceptance checks. |
| `stage` | One `Stage.do_stage(...)` call, after the existing `Running ...` message and telemetry start event. Excludes grouping/sorting, collecting the return value, and output writing. The `stage` tag uses the existing stage name. |
| `calibration_selection` | `get_calibration_file_info(image)`, including master selection in the database and conversion to file information. |
| `calibration_open` | One master `frame_factory.open(...)` call, including reads/decoding, construction/memmaps, and factory checks. Excludes setting `is_master` and the existing calibration-frame-ID persistence step; those remain inside `stage`. |
| `calibration_apply` | `apply_master_calibration(image, master)` only. Missing-master handling is not calibration application. |
| `output_write` | One entire `image.write(runtime_context)` call after **all** stages. Includes all preparation, products, I/O and bookkeeping performed by that implementation, including calibration-record writing for calibration frames. |
| `output_metadata` | `save_processing_metadata(...)`: update the processing date, reduction level, pipeline version, and public date as already implemented. |
| `output_preparation` | Determine the output filename, build FITS HDUs through `to_fits(...)` (including existing datatype conversion and packing), and determine the output directory. Excludes serialization into `BytesIO`. |
| `fits_serialization` | The entire unchanged `DataProduct.from_fits(...)` call: allocate `BytesIO`, `HDUList.writeto(..., output_verify='silentfix')`, rewind the buffer, and construct the `DataProduct`. |
| `archive_upload` | `post_to_ingester(...)`, including existing retries/backoff, followed by extracting/validating the returned `frameid`. Emitted only when archive posting executes. An absent `frameid` is an error, preserving the existing exception. |
| `file_cache_write` | Create the directory, rewind/read the buffer, open/write the output file once, and close it. Emitted only when file caching executes. Does not add an `fsync`. |
| `checksum` | Rewind and read the buffer again, calculate MD5, and produce the hex digest. |
| `processed_image_record` | `dbs.save_processed_image(...)`, including its processed-image lookup and database commit. |
| `reduction_total` | From just before frame-factory construction in `run_pipeline_stages` through input opening, stage selection/execution, and return from the last `image.write(...)`, or until an early return/exception. Includes processed-image recording and any archive upload in that write. |

The total excludes caller work: queue wait, task eligibility/retry accounting,
the preliminary header-only read in `process_stackframe`, task completion flags,
Smartstack membership/upsert bookkeeping after `run_pipeline_stages`, and later
Smartstack combination, previews, and shipping. It is a **pipeline reduction**
duration, not end-to-end task or exposure latency. Tiny timing/tag/outcome
bookkeeping inside a measured block and nested logging are included; emitting
the block's own timing record is outside that block's duration.

The output implementation still serializes FITS once into `BytesIO`, reads that
buffer to write the cached file once, and reads it again for MD5. No cache,
calibration selection, stage, FITS product, or resource configuration changes
are part of this instrumentation.

## Nesting, outcomes, and grouped processing

**Durations overlap. Do not add parent totals to their children.**

```text
reduction_total
  input_open (once per supplied input)
  stage (once per frame or group, for each stage)
    calibration_selection
    calibration_open
    calibration_apply
  output_write (once per surviving output)
    output_metadata
    output_preparation
    fits_serialization
    archive_upload (optional)
    file_cache_write (optional)
    checksum
    processed_image_record
```

Children need not sum exactly to their parent: stage dispatch, logging,
calibration frame-ID persistence, and other work between blocks are measured
only by the relevant enclosing block.

- `success`: the operation returned normally. For stages and calibration
  application it returned a non-`None` frame; this is not a claim that all
  scientific quality checks passed.
- `error`: the block raised. The same exception is re-raised to the existing
  handler. In particular, `Stage.run` still catches stage exceptions, logs
  `Reduction stopped`, and continues with other frames/groups.
- `rejected`: opening or a stage/application returned `None`, or no inputs
  survived opening for a pipeline total.
- `missing`: calibration selection found no master. Existing `override_missing`
  and missing-master behavior still decide whether reduction continues.
- `stopped`: a pipeline total ended early because a stage left no outputs.
  Consult the stage records to distinguish rejection from a caught exception.
- `outputs_written`: a pipeline reached the end of output writing. For a batch
  this explicitly does **not** assert that every input survived; inspect its
  per-input/per-group records.

`reduction_total.scope=frame` is emitted only for one input with
`calibration_maker=False`. Multiple inputs, empty inputs, and calibration-maker
calls have `scope=batch`, `input_filenames`, and `input_count`. Totals also
report `opened_count` after opening and `output_count` after all writes succeed.
A batch total is wall time for the whole invocation, never a per-frame latency
or an average. Filtering, sorting, grouping, and return values are unchanged.

`stage.scope=group` measures a single call with the entire group and carries
`input_count` and all `input_filenames`. This includes groups of size one.
Other image tags on a group stage describe its **first member**, matching the
existing stage log; they must not be treated as properties of every member.
Ungrouped stage calls have `scope=frame` and `input_count=1`.

## Example records

Illustrative tag excerpts (durations are examples, not CPT measurements):

```json
{"event":"reduction_timing","operation":"input_open","outcome":"success","duration_s":1.24,"input_filename":"member-e00.fits","request_num":123,"camera":"sq39","processName":"ForkPoolWorker-1","process_id":81,"configuration_mode":"central30x30","observing_mode":"NORMAL","image_shapes":[[2048,2048]],"smartstack_moluid":"example-mol","smartstack_stack_num":2,"smartstack_frmtotal":3}
{"event":"reduction_timing","operation":"calibration_open","outcome":"success","duration_s":0.43,"input_filename":"member-e00.fits","calibration_filename":"master-bias.fits","stage":"banzai.stages.BiasSubtractor","calibration_type":"BIAS","camera":"sq39","process_id":81}
{"event":"reduction_timing","operation":"fits_serialization","outcome":"success","duration_s":0.28,"input_filename":"member-e00.fits","output_filename":"member-e09.fits","camera":"sq39","process_id":81}
{"event":"reduction_timing","operation":"reduction_total","scope":"frame","outcome":"outputs_written","duration_s":8.72,"input_filename":"member-e00.fits","input_count":1,"opened_count":1,"output_count":1,"camera":"sq39","process_id":81}
```

For CPT profiling, select `event=reduction_timing`, single-frame
`reduction_total` records with `outcome=outputs_written`, and raw input
`reduction_level=0`. Group by configuration and initial shape; join stage and
output details by input filename, process identity, Smartstack member identity,
and the surrounding log timestamps. Retries may produce multiple attempts for
the same filename, so do not collapse them into a single sample.
