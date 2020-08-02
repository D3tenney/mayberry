-- 20200523 NOTE: Started altering this script to
--                store changes in Postgres DB Table voter_change.
--                Leaving to solve issue with s3/Athena.

-- Remove Dropped Voters
INSERT INTO voter_removed
SELECT voter.*, CURRENT_DATE
FROM voter
LEFT JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter_staging.ncid IS NULL;

WITH removed_voters AS (
  SELECT voter.ncid
  FROM voter
  LEFT JOIN voter_staging
    ON voter.ncid= voter_staging.ncid
  WHERE voter_staging.ncid IS NULL
)

DELETE FROM voter
USING removed_voters
WHERE voter.ncid = removed_voters.ncid;

-- Add New Voters
INSERT INTO voter
SELECT voter_staging.*
FROM voter_staging
LEFT JOIN voter
  ON voter_staging.ncid = voter.ncid
WHERE voter.ncid IS NULL;

WITH new_voters AS (
  SELECT voter_staging.ncid
  FROM voter_staging
  LEFT JOIN voter
    ON voter_staging.ncid = voter.ncid
  WHERE voter.ncid IS NULL
)

DELETE FROM voter_staging
USING new_voters
WHERE voter_staging.ncid = new_voters.ncid;

--Reg Num Change
INSERT INTO voter_change
SELECT voter.ncid, 'voter reg num', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.voter_reg_num != voter_staging.voter_reg_num;

--Reg Date Change
INSERT INTO voter_change
SELECT voter.*, 'registration date', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.registr_dt != voter_staging.registr_dt;

--Name Change
INSERT INTO voter_change
SELECT voter.*, 'name', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.first_name != voter_staging.first_name OR
      voter.middle_name != voter_staging.middle_name OR
      voter.last_name != voter_staging.last_name;

-- Registration Address
INSERT INTO voter_change
SELECT voter.*, 'registration address', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.res_street_address != voter_staging.res_street_address OR
      voter.res_city_desc != voter_staging.res_city_desc OR
      voter.state_cd != voter_staging.state_cd OR
      voter.zip_code != voter_staging.zip_code;

-- Mail Address
INSERT INTO voter_change
SELECT voter.*, 'mailing address', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.mail_addr1 != voter_staging.mail_addr1 OR
      voter.mail_addr2 != voter_staging.mail_addr2 OR
      voter.mail_addr3 != voter_staging.mail_addr3 OR
      voter.mail_addr4 != voter_staging.mail_addr4 OR
      voter.mail_city != voter_staging.mail_city OR
      voter.mail_state != voter_staging.mail_state OR
      voter.mail_zipcode != voter_staging.mail_zipcode;

-- Party Affiliation
INSERT INTO voter_change
SELECT voter.*, 'party affiliation', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.party_cd != voter_staging.party_cd;

-- Absentee
INSERT INTO voter_change
SELECT voter.*, 'absentee status', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.absent_ind != voter_staging.absent_ind;

-- Political Geography (county, precinct, cd, sd, ld)
INSERT INTO voter_change
SELECT voter.*, 'political geography', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.county_id != voter_staging.county_id OR
      voter.precinct_abbrv != voter_staging.precinct_abbrv OR
      voter.cong_dist_abbrv != voter_staging.cong_dist_abbrv OR
      voter.nc_senate_abbrv != voter_staging.nc_senate_abbrv OR
      voter.nc_house_abbrv != voter_staging.nc_house_abbrv;

-- status/reason change
INSERT INTO voter_change
SELECT voter.*, 'voter status, reason', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.status_cd != voter_staging.state_cd OR
      voter.reason_cd != voter_staging.reason_cd;

-- Driver License
INSERT INTO voter_change
SELECT voter.*, 'driver license', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.drivers_lic != voter_staging.drivers_lic;

-- Phone Number
INSERT INTO voter_change
SELECT voter.*, 'phone number', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.full_phone_number != voter_staging.full_phone_number;

-- Demographic (gender, race, ethnicity)
INSERT INTO voter_change
SELECT voter.*, 'demographics', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.gender_code != voter_staging.gender_code OR
      voter.race_code != voter_staging.race_code OR
      voter.ethnic_code != voter_staging.ethnic_code;

-- Birth
INSERT INTO voter_change
SELECT voter.*, 'birth', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.birth_year != voter_staging.birth_year OR
      voter.birth_state != voter_staging.birth_state;

INSERT INTO voter
SELECT *
FROM voter_staging
ON CONFLICT DO UPDATE
SET

-- Drop unchanged rows
DELETE FROM voter_staging
USING voter
WHERE voter_staging.ncid = voter.ncid
  AND voter_staging.voter_reg_num = voter.voter_reg_num
  AND voter_staging.registr_dt = voter.registr_dt
  AND voter_staging.first_name = voter.first_name
  AND voter_staging.middle_name = voter.middle_name
  AND voter_staging.last_name = voter.last_name
  AND voter_staging.res_street_address = voter.res_street_address
  AND voter_staging.res_city_desc = voter.res_city_desc
  AND voter_staging.state_cd = voter.state_cd
  AND voter_staging.zip_code = voter.zip_code
  AND voter_staging.mail_addr1 = voter.mail_addr1
  AND voter_staging.mail_addr2 = voter.mail_addr2
  AND voter_staging.mail_addr3 = voter.mail_addr3
  AND voter_staging.mail_addr4 = voter.mail_addr4
  AND voter_staging.mail_city = voter.mail_city
  AND voter_staging.mail_state = voter.mail_state
  AND voter_staging.mail_zipcode = voter.mail_zipcode
  AND voter_staging.party_cd = voter.party_cd
  AND voter_staging.absent_ind = voter.absent_ind
  AND voter_staging.county_id = voter.county_id
  AND voter_staging.precinct_abbrv = voter.precinct_abbrv
  AND voter_staging.cong_dist_abbrv = voter.cong_dist_abbrv
  AND voter_staging.nc_senate_abbrv = voter.nc_senate_abbrv
  AND voter_staging.nc_house_abbrv = voter.nc_house_abbrv
  AND voter_staging.status_cd = voter.status_cd
  AND voter_staging.reason_cd = voter.reason_cd
  AND voter_staging.drivers_lic = voter.drivers_lic
  AND voter_staging.full_phone_number = voter.full_phone_number
  AND voter_staging.gender_code = voter.gender_code
  AND voter_staging.race_code = voter.race_code
  AND voter_staging.ethnic_code = voter.ethnic_code
  -- AND voter_staging.birth_age = voter.birth_age
  AND voter_staging.birth_year = voter.birth_year
  AND voter_staging.birth_state = voter.birth_state;

--Reg Num Change
INSERT INTO voter_past
SELECT voter.*, 'voter reg num', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.voter_reg_num != voter_staging.voter_reg_num;

WITH reg_num_change AS (
  SELECT voter.ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.voter_reg_num != voter_staging.voter_reg_num
)

DELETE FROM voter
USING reg_num_change
WHERE voter.ncid = reg_num_change.ncid;

--Reg Date Change
INSERT INTO voter_past
SELECT voter.*, 'registration date', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.registr_dt != voter_staging.registr_dt;

WITH reg_date_change AS (
  SELECT voter.ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.registr_dt != voter_staging.registr_dt
)

DELETE FROM voter
USING reg_date_change
WHERE voter.ncid = reg_date_change.ncid;

--Name Change
INSERT INTO voter_past
SELECT voter.*, 'name', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.first_name != voter_staging.first_name OR
      voter.middle_name != voter_staging.middle_name OR
      voter.last_name != voter_staging.last_name;

WITH name_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.first_name != voter_staging.first_name OR
        voter.middle_name != voter_staging.middle_name OR
        voter.last_name != voter_staging.last_name
)

DELETE FROM voter
USING name_change
WHERE voter.ncid = name_change.ncid;

-- Registration Address
INSERT INTO voter_past
SELECT voter.*, 'registration address', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.res_street_address != voter_staging.res_street_address OR
      voter.res_city_desc != voter_staging.res_city_desc OR
      voter.state_cd != voter_staging.state_cd OR
      voter.zip_code != voter_staging.zip_code;

WITH reg_addr_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.res_street_address != voter_staging.res_street_address OR
        voter.res_city_desc != voter_staging.res_city_desc OR
        voter.state_cd != voter_staging.state_cd OR
        voter.zip_code != voter_staging.zip_code
)

DELETE FROM voter
USING reg_addr_change
WHERE voter.ncid = reg_addr_change.ncid;

-- Mail Address
INSERT INTO voter_past
SELECT voter.*, 'mailing address', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.mail_addr1 != voter_staging.mail_addr1 OR
      voter.mail_addr2 != voter_staging.mail_addr2 OR
      voter.mail_addr3 != voter_staging.mail_addr3 OR
      voter.mail_addr4 != voter_staging.mail_addr4 OR
      voter.mail_city != voter_staging.mail_city OR
      voter.mail_state != voter_staging.mail_state OR
      voter.mail_zipcode != voter_staging.mail_zipcode;

WITH mail_addr_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.mail_addr1 != voter_staging.mail_addr1 OR
        voter.mail_addr2 != voter_staging.mail_addr2 OR
        voter.mail_addr3 != voter_staging.mail_addr3 OR
        voter.mail_addr4 != voter_staging.mail_addr4 OR
        voter.mail_city != voter_staging.mail_city OR
        voter.mail_state != voter_staging.mail_state OR
        voter.mail_zipcode != voter_staging.mail_zipcode
)

DELETE FROM voter
USING mail_addr_change
WHERE voter.ncid = mail_addr_change.ncid;

-- Party Affiliation
INSERT INTO voter_past
SELECT voter.*, 'party affiliation', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.party_cd != voter_staging.party_cd;

WITH party_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.party_cd != voter_staging.party_cd
)

DELETE FROM voter
USING party_change
WHERE voter.ncid = party_change.ncid;

-- Absentee
INSERT INTO voter_past
SELECT voter.*, 'absentee status', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.absent_ind != voter_staging.absent_ind;

WITH absentee_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.absent_ind != voter_staging.absent_ind
)

DELETE FROM voter
USING absentee_change
WHERE voter.ncid = absentee_change.ncid;

-- Political Geography (county, precinct, cd, sd, ld)
INSERT INTO voter_past
SELECT voter.*, 'political geography', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.county_id != voter_staging.county_id OR
      voter.precinct_abbrv != voter_staging.precinct_abbrv OR
      voter.cong_dist_abbrv != voter_staging.cong_dist_abbrv OR
      voter.nc_senate_abbrv != voter_staging.nc_senate_abbrv OR
      voter.nc_house_abbrv != voter_staging.nc_house_abbrv;

WITH pol_geo_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.county_id != voter_staging.county_id OR
        voter.precinct_abbrv != voter_staging.precinct_abbrv OR
        voter.cong_dist_abbrv != voter_staging.cong_dist_abbrv OR
        voter.nc_senate_abbrv != voter_staging.nc_senate_abbrv OR
        voter.nc_house_abbrv != voter_staging.nc_house_abbrv
)

DELETE FROM voter
USING pol_geo_change
WHERE voter.ncid = pol_geo_change.ncid;

-- status/reason change
INSERT INTO voter_past
SELECT voter.*, 'voter status, reason', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.status_cd != voter_staging.state_cd OR
      voter.reason_cd != voter_staging.reason_cd;

WITH status_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.status_cd != voter_staging.state_cd OR
        voter.reason_cd != voter_staging.reason_cd
)

DELETE FROM voter
USING status_change
WHERE voter.ncid = status_change.ncid;

-- Driver License
INSERT INTO voter_past
SELECT voter.*, 'driver license', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.drivers_lic != voter_staging.drivers_lic;

WITH license_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.drivers_lic != voter_staging.drivers_lic
)

DELETE FROM voter
USING license_change
WHERE voter.ncid = license_change.ncid;

-- Phone Number
INSERT INTO voter_past
SELECT voter.*, 'phone number', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.full_phone_number != voter_staging.full_phone_number;

WITH phone_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.full_phone_number != voter_staging.full_phone_number
)

DELETE FROM voter
USING phone_change
WHERE voter.ncid = phone_change.ncid;

-- Demographic (gender, race, ethnicity)
INSERT INTO voter_past
SELECT voter.*, 'demographics', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.gender_code != voter_staging.gender_code OR
      voter.race_code != voter_staging.race_code OR
      voter.ethnic_code != voter_staging.ethnic_code;

WITH demographic_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.gender_code != voter_staging.gender_code OR
        voter.race_code != voter_staging.race_code OR
        voter.ethnic_code != voter_staging.ethnic_code
)

DELETE FROM voter
USING demographic_change
WHERE voter.ncid = demographic_change.ncid;

-- Birth
INSERT INTO voter_past
SELECT voter.*, 'birth', CURRENT_DATE
FROM voter
JOIN voter_staging
  ON voter.ncid = voter_staging.ncid
WHERE voter.birth_year != voter_staging.birth_year OR
      voter.birth_state != voter_staging.birth_state;

WITH birth_change AS (
  SELECT ncid
  FROM voter
  JOIN voter_staging
    ON voter.ncid = voter_staging.ncid
  WHERE voter.birth_year != voter_staging.birth_year OR
        voter.birth_state != voter_staging.birth_state
)

DELETE FROM voter
USING birth_change
WHERE voter.ncid = birth_change.ncid;

-- Updated Birth Age
--UPDATE voter
--SET birth_age = voter_staging.birth_age
--FROM voter_staging
--WHERE voter.ncid = voter_staging.ncid AND
--      voter.birth_age != voter_staging.birth_age

-- Insert remaining rows
INSERT INTO voter
SELECT voter_staging.*
FROM voter_staging
LEFT JOIN voter
  ON voter_staging.ncid = voter.ncid
WHERE voter.ncid IS NULL;

--TRUNCATE voter_staging;
