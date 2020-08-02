-- REFERENCE TABLES

CREATE TABLE IF NOT EXISTS county (
    id SMALLINT PRIMARY KEY,
    name TEXT
    );

CREATE TABLE IF NOT EXISTS precinct (
    county_id SMALLINT NOT NULL REFERENCES county(id),
    abbrv TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (county_id, abbrv)
    );

CREATE TABLE IF NOT EXISTS voter_status (
  cd TEXT PRIMARY KEY,
  description TEXT
);

CREATE TABLE IF NOT EXISTS voter_status_reason (
  cd TEXT PRIMARY KEY,
  description TEXT
);

CREATE TABLE IF NOT EXISTS race (
  code TEXT PRIMARY KEY,
  description TEXT
);

CREATE TABLE IF NOT EXISTS ethnicity (
  code TEXT PRIMARY KEY,
  description TEXT
);

CREATE TABLE IF NOT EXISTS party (
  cd TEXT PRIMARY KEY,
  description TEXT
);

-- VOTER, VOTER_STAGING, and VOTER_OLD

CREATE TABLE IF NOT EXISTS voter (
    ncid TEXT NOT NULL PRIMARY KEY,
    voter_reg_num TEXT NOT NULL,
    first_name TEXT NOT NULL,
    middle_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    status_cd TEXT REFERENCES voter_status(cd),
    reason_cd TEXT REFERENCES voter_status_reason(cd),
    absent_ind TEXT NOT NULL,
    res_street_address TEXT NOT NULL,
    res_city_desc TEXT NOT NULL,
    state_cd TEXT NOT NULL,
    zip_code TEXT NOT NULL,
    mail_addr1 TEXT NOT NULL,
    mail_addr2 TEXT NOT NULL,
    mail_addr3 TEXT NOT NULL,
    mail_addr4 TEXT NOT NULL,
    mail_city TEXT NOT NULL,
    mail_state TEXT NOT NULL,
    mail_zipcode TEXT NOT NULL,
    full_phone_number TEXT NOT NULL,
    drivers_lic TEXT NOT NULL,
    race_code TEXT NOT NULL REFERENCES race(code),
    ethnic_code TEXT NOT NULL REFERENCES ethnicity(code),
    party_cd TEXT NOT NULL REFERENCES party(cd),
    gender_code TEXT NOT NULL,
    birth_year SMALLINT NOT NULL,
    --birth_age SMALLINT,
    birth_state TEXT NOT NULL,
    registr_dt DATE NOT NULL,
    county_id SMALLINT NOT NULL REFERENCES county(id),
    precinct_abbrv TEXT NOT NULL,
    -- precinct_desc,
    -- municipality_abbrv TEXT,
    -- municipality_desc,
    -- ward_abbrv TEXT,
    -- ward_desc,
    -- school_dist_abbrv TEXT,
    -- school_dist_desc
    cong_dist_abbrv TEXT NOT NULL,
    nc_senate_abbrv TEXT NOT NULL,
    nc_house_abbrv TEXT NOT NULL,
    -- super_court_abbrv TEXT
    vintage_month TEXT NOT NULL DEFAULT LEFT(CAST(CURRENT_DATE AS TEXT), 7)
    );

CREATE TABLE IF NOT EXISTS voter_staging (
  LIKE voter INCLUDING ALL,
  CONSTRAINT voter_county_id_fkey FOREIGN KEY (county_id)
    REFERENCES public.county (id) MATCH SIMPLE,
  CONSTRAINT voter_ethnic_code_fkey FOREIGN KEY (ethnic_code)
    REFERENCES public.ethnicity (code) MATCH SIMPLE,
  CONSTRAINT voter_party_cd_fkey FOREIGN KEY (party_cd)
    REFERENCES public.party (cd) MATCH SIMPLE,
  CONSTRAINT voter_race_code_fkey FOREIGN KEY (race_code)
    REFERENCES public.race (code) MATCH SIMPLE,
  CONSTRAINT voter_reason_cd_fkey FOREIGN KEY (reason_cd)
    REFERENCES public.voter_status_reason (cd) MATCH SIMPLE,
  CONSTRAINT voter_status_cd_fkey FOREIGN KEY (status_cd)
    REFERENCES public.voter_status (cd) MATCH SIMPLE
);

CREATE TABLE IF NOT EXISTS voter_removed (
  LIKE voter,
  inserted_on DATE DEFAULT CURRENT_DATE,
  PRIMARY KEY(ncid, inserted_on),
  CONSTRAINT voter_county_id_fkey FOREIGN KEY (county_id)
    REFERENCES public.county (id) MATCH SIMPLE,
  CONSTRAINT voter_ethnic_code_fkey FOREIGN KEY (ethnic_code)
    REFERENCES public.ethnicity (code) MATCH SIMPLE,
  CONSTRAINT voter_party_cd_fkey FOREIGN KEY (party_cd)
    REFERENCES public.party (cd) MATCH SIMPLE,
  CONSTRAINT voter_race_code_fkey FOREIGN KEY (race_code)
    REFERENCES public.race (code) MATCH SIMPLE,
  CONSTRAINT voter_reason_cd_fkey FOREIGN KEY (reason_cd)
    REFERENCES public.voter_status_reason (cd) MATCH SIMPLE,
  CONSTRAINT voter_status_cd_fkey FOREIGN KEY (status_cd)
    REFERENCES public.voter_status (cd) MATCH SIMPLE
);

CREATE TABLE IF NOT EXISTS voter_change (
  ncid TEXT NOT NULL,
  inserted_on DATE DEFAULT CURRENT_DATE,
  reason TEXT NOT NULL,
  old_data JSON NOT NULL,
  PRIMARY KEY (ncid, inserted_on, reason)
);

-- PARTY CHANGE

CREATE TABLE IF NOT EXISTS party_change (
  voter_reg_num TEXT NOT NULL,
  change_dt DATE NOT NULL,
  party_from TEXT,
  party_to TEXT,
  county_id SMALLINT NOT NULL REFERENCES county(id),
  PRIMARY KEY (voter_reg_num, change_dt)
);

CREATE TABLE IF NOT EXISTS party_change_staging (
  LIKE party_change INCLUDING ALL,
  CONSTRAINT party_change_county_fkey FOREIGN KEY (county_id)
    REFERENCES public.county (id) MATCH SIMPLE
);

-- VOTE HISTORY

CREATE TABLE IF NOT EXISTS vote_history (
  ncid TEXT NOT NULL,
  voter_reg_num TEXT NOT NULL,
  election_lbl DATE NOT NULL,
  election_desc TEXT NOT NULL,
  county_id SMALLINT NOT NULL REFERENCES county(id),
  voted_county_id SMALLINT NOT NULL REFERENCES county(id),
  pct_label TEXT NOT NULL,
  voted_party_cd TEXT NOT NULL REFERENCES party(cd),
  voting_method TEXT NOT NULL,
  vtd_label TEXT,
  vtd_description TEXT,
  PRIMARY KEY (ncid, election_lbl, election_desc)
);

CREATE TABLE IF NOT EXISTS vote_history_staging (
  LIKE vote_history INCLUDING ALL,
  CONSTRAINT vote_history_county_id_fkey FOREIGN KEY (county_id)
    REFERENCES public.county (id) MATCH SIMPLE,
  CONSTRAINT vote_history_voted_county_id_fkey FOREIGN KEY (county_id)
    REFERENCES public.county (id) MATCH SIMPLE,
  CONSTRAINT vote_history_voted_party_cd_fkey FOREIGN KEY (voted_party_cd)
    REFERENCES public.party (cd) MATCH SIMPLE
);
