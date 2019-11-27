create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.click (
  record_type string,
  date string,
  time string,
  idevent_type string,
  placementid string,
  ipn string,
  idcreative string,
  configuration_id string,
  GUID string,
  iab_flag string,
  ip_address string,
  rule_match string,
  custom string,
  section string,
  keyword string,
  privacy string,
  parent_time string,
  device_id string,
  imp_id string,
  agent_env string,
  user_agent string,
  impression_guid string,
  unhex_md5_smartclip string, --this will be unhex_md5_smartclip
  idcampaign string,
  c2 string,
  c3 string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_click_2019070415;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.click
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.click (
  record_type,
  date,
  time,
  idevent_type,
  placementid,
  ipn,
  idcreative,
  configuration_id,
  GUID,
  iab_flag,
  ip_address,
  rule_match,
  custom,
  section,
  keyword,
  privacy,
  parent_time,
  device_id,
  imp_id,
  agent_env,
  user_agent,
  impression_guid,
  unhex_md5_smartclip, --this will be unhex_md5_smartclip
  idcampaign,
  c2,
  c3,
  file_source,
  load_timestamp,
  run_datehour
)

select
  record_type,
  date,
  time,
  idevent_type,
  placementid,
  ipn,
  idcreative,
  configuration_id,
  GUID,
  iab_flag,
  ip_address,
  rule_match,
  custom,
  section,
  keyword,
  privacy,
  parent_time,
  device_id,
  imp_id,
  agent_env,
  user_agent,
  impression_guid,
  unhex_md5_smartclip, --this will be unhex_md5_smartclip
  idcampaign,
  c2,
  c3,
  file_source,
  load_timestamp,
  run_datehour
from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.click
where run_datehour = 2019070415;

---

commit;
