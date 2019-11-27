create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.int (
  ipa string,
  date_field string,
  time_field string,
  agent_string string,
  idad_index string,
  idcreative_config string,
  placementid string,
  smartclip string,
  idevent_type string,
  sid string,
  guid string,
  unhex_md5_smartclip string,
  options string,
  file_source string,
  load_timestamp string,
  run_datehour bigint
)
;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.int
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.int (
  ipa,
  date_field,
  time_field,
  agent_string,
  idad_index,
  idcreative_config,
  placementid,
  smartclip,
  idevent_type,
  sid,
  guid,
  unhex_md5_smartclip,
  options,
  file_source,
  load_timestamp,
  run_datehour
)

select
  ipa,
  date_field,
  time_field,
  agent_string,
  idad_index,
  idcreative_config,
  placementid,
  smartclip,
  idevent_type,
  sid,
  guid,
  unhex_md5_smartclip,
  options,
  file_source,
  load_timestamp,
  run_datehour
  from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.int
  where run_datehour = 2019070415;
