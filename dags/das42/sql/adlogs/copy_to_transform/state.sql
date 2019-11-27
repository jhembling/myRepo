create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.state (
  record_type string,
  date string,
  time string,
  event string,
  ipn string,
  iab_flag string,
  config_id string,
  impression_id string,
  ip_address string,
  product string,
  data string,
  impression_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.state
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.state (
  record_type,
  date,
  time,
  event,
  ipn,
  iab_flag,
  config_id,
  impression_id,
  ip_address,
  product,
  data,
  impression_guid,
  file_source,
  load_timestamp,
  run_datehour
)

select
  record_type,
  date,
  time,
  event,
  ipn,
  iab_flag,
  config_id,
  impression_id,
  ip_address,
  product,
  data,
  impression_guid,
  file_source,
  load_timestamp,
  run_datehour
  from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.state
  where run_datehour = 2019070415;
