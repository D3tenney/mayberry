
"""
VOTER_DTYPES = {'ncid': object, 'voter_reg_num': object,
                'first_name': object, 'middle_name': object,
                'last_name': object,
                'status_cd': object, 'reason_cd': object,
                'absent_ind': object,
                'res_street_address': object,
                'res_city_desc': object, 'state_cd': object,
                'zip_code': object,
                'mail_addr1': object, 'mail_addr2': object,
                'mail_addr3': object, 'mail_addr4': object,
                'mail_city': object, 'mail_state': object,
                'mail_zipcode': object,
                'full_phone_number': object, 'drivers_lic': object,
                'race_code': object, 'ethnic_code': object,
                'party_cd': object,
                'gender_code': object, 'birth_year': int,  # 'birth_age': int,
                'birth_state': object,
                'registr_dt': str,
                'county_id': int, 'precinct_abbrv': object,
                'cong_dist_abbrv': object, 'nc_senate_abbrv': object,
                'nc_house_abbrv': object}

VOTER_SUBSET = ['ncid', 'voter_reg_num',
                'first_name', 'middle_name', 'last_name',
                'status_cd', 'reason_cd',
                'absent_ind',
                'res_street_address', 'res_city_desc', 'state_cd', 'zip_code',
                'mail_addr1', 'mail_addr2', 'mail_addr3', 'mail_addr4',
                'mail_city', 'mail_state', 'mail_zipcode',
                'full_phone_number', 'drivers_lic',
                'race_code', 'ethnic_code', 'party_cd',
                'gender_code', 'birth_year',  # 'birth_age',
                               'birth_state',
                'registr_dt',
                'county_id', 'precinct_abbrv',
                'cong_dist_abbrv', 'nc_senate_abbrv', 'nc_house_abbrv']
"""

VOTER_DTYPES = {'ncid': object, 'voter_reg_num': object,
                'status_cd': object, 'absent_ind': object,
                'state_cd': object, 'mail_state': object,
                'res_city_desc': object, 'mail_city': object,
                'drivers_lic': object, 'race_code': object,
                'ethnic_code': object, 'party_cd': object,
                'gender_code': object, 'county_id': int,
                'cong_dist_abbrv': object, 'nc_senate_abbrv': object,
                'nc_house_abbrv': object,
                'registr_dt': str}

VOTE_HISTORY_READ_COLS = ['ncid', 'election_lbl', 'election_desc']

VOTER_READ_COLS = ['ncid',
                   # --geography
                   'county_id', 'cong_dist_abbrv',
                   'nc_senate_abbrv', 'nc_house_abbrv',
                   'res_city_desc',
                   # --political
                   'party_cd',
                   # --demographic
                   'gender_code', 'race_code', 'ethnic_code',
                   # --individual status
                   'status_cd', 'absent_ind']

VOTER_PARTITION_COLS = [
                        # --geography
                        'county_id', 'cong_dist_abbrv',
                        'nc_senate_abbrv', 'nc_house_abbrv',
                        'res_city_desc',
                        # --political
                        'party_cd',
                        # --demographic
                        'gender_code', 'race_code', 'ethnic_code',
                        # --individual status
                        'status_cd', 'absent_ind',
                        # 'drivers_lic'
                        'prior_voter', 'primary_voter'
                        ]

VOTER_NON_PARTITION_COLS = ['ncid', 'voter_reg_num', 'registr_dt']

"""
VOTER_SUBSET = [
                # partition columns
                # --geography
                'county_id', 'cong_dist_abbrv',
                'nc_senate_abbrv', 'nc_house_abbrv',
                'res_city_desc',
                # --political
                'party_cd',
                # --demographic
                'gender_code', 'race_code', 'ethnic_code'
                # --individual status
                'status_cd', 'absent_ind',
                'drivers_lic',
                # file columns
                'ncid', 'voter_reg_num',
                'registr_dt']
"""
HELPER_SUBSET = ['county_id', 'county_desc',
                 'ethnic_code',
                 'race_code',
                 'party_cd',
                 'precinct_abbrv', 'precinct_desc',
                 'status_cd', 'voter_status_desc',
                 'reason_cd',
                 'voter_status_reason_desc']

HELPER_DESCRIPTIONS = {'ethnicity': {'HL': 'HISPANIC OR LATINO',
                                     'NL': 'NOT HISPANIC OR LATINO',
                                     'UN': 'NOT PROVIDED'},
                       'race': {'': 'None',
                                'A': 'ASIAN',
                                'B': 'BLACK OR AFRICAN AMERICAN',
                                'I': 'AMERICAN INDIAN OR ALASKA NATIVE',
                                'M': 'TWO OR MORE RACES',
                                'O': 'OTHER',
                                'U': 'UNDESIGNATED',
                                'W': 'WHITE'},
                       'party': {'CST': 'CONSTITUTION PARTY',
                                 'DEM': 'DEMOCRATIC PARTY',
                                 'GRE': 'GREEN PARTY',
                                 'LIB': 'LIBERTARIAN PARTY',
                                 'REP': 'REPUBLICAN PARTY',
                                 'UNA': 'UNAFFILIATED'}}

REMOVED_VOTER_NULL_COLS = ['res_city_desc', 'state_cd', 'zip_code',
                           'mail_addr1', 'mail_addr2', 'mail_addr3',
                           'mail_addr4', 'mail_state', 'mail_zipcode',
                           'full_phone_number', 'precinct_abbrv',
                           'cong_dist_abbrv', 'nc_senate_abbrv',
                           'nc_house_abbrv']

NULL_REPLACE_COLS = ['first_name', 'middle_name', 'last_name',
                     'res_street_address', 'res_city_desc',
                     'state_cd', 'zip_code', 'mail_addr1', 'mail_addr2',
                     'mail_addr3', 'mail_addr4', 'mail_city',
                     'mail_state', 'mail_zipcode', 'full_phone_number',
                     'gender_code', 'birth_state', 'precinct_abbrv',
                     'cong_dist_abbrv', 'nc_senate_abbrv', 'nc_house_abbrv']
