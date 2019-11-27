create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.track (
  record_type string,
  date string,
  time string,
  event string,
  spotlight_id string,
  spotlightgroup_id string,
  track_id string,
  ip_address string,
  product string,
  data string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.track
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.track (
  record_type,
  date,
  time,
  event,
  spotlight_id,
  spotlightgroup_id,
  track_id,
  ip_address,
  product,
  data,
  spotlight_request_guid,
  file_source,
  load_timestamp,
  run_datehour
)

select
  record_type,
  date,
  time,
  event,
  spotlight_id,
  spotlightgroup_id,
  track_id,
  ip_address,
  product,
  data,
  spotlight_request_guid,
  file_source,
  load_timestamp,
  run_datehour
from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.track
where run_datehour = 2019070415 ;
