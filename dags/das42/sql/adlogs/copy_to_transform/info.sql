create table if not exists airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.info (
  record_type string,
  date string,
  time string,
  error_severity string,
  error_number string,
  placement_id string,
  error_message string,
  ip_address string,
  referrer string,
  iab_flag string,
  user_agent string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
)
;

---

begin name load_rl_info_2019070415;

---

delete from airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.info
where run_datehour = 2019070415
;

---

insert into airflow_db_{{ params.env }}.transform_stage_{{ params.team_name }}.info (
  record_type,
  date,
  time,
  error_severity,
  error_number,
  placement_id,
  error_message,
  ip_address,
  referrer,
  iab_flag,
  user_agent,
  file_source,
  load_timestamp,
  run_datehour
)

select
  record_type,
  date,
  time,
  error_severity,
  error_number,
  placement_id,
  error_message,
  ip_address,
  referrer,
  iab_flag,
  user_agent,
  file_source,
  load_timestamp,
  run_datehour
from airflow_db_{{ params.env }}.raw_stage_{{ params.team_name }}.info
where run_datehour = 2019070415;

---

commit;
