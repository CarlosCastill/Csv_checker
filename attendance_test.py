# To run this scrip (python3 attendance_test.py -f attendances_20201208.csv )
import pandas as pd
import argparse
import sys
from pathlib import Path
from cerberus import Validator
import os

def create_args(args):
    # Function will validate .csv file and columns
    if args.filepath:
        try:
            #employees = pd.read_csv(args.filepath, parse_dates=['birth_date', 'status_date']).fillna("")
            print("[*] User input validated")
            filename = (args.filepath)
        except Exception:
            raise ValueError("[*] File Format Issue - Please input a valid .csv file.")
    else:
        print("Please input a filepath with tag -f")

    print("[*] Now running '{}'".format(__file__))

    # if Employees file
    if str(filename).startswith("employees"):
        employees = pd.read_csv(args.filepath, parse_dates=['birth_date', 'status_date']).fillna("")
        # check employee file
        compute_employees(employees)

        print("[*] Compute successful")

        # Output the parsed employees file
        employees.to_csv(filename, index=False)


    # if Earnings file
    elif str(filename).startswith("earnings"):
        earnings = pd.read_csv(args.filepath, parse_dates=['check_date', 'period_start_date', 'period_end_date']).fillna("")
        # check earnings file
        compute_earnings(earnings)

        print("[*] Compute successful")

        # Output the parsed earnings file
        #earnings.to_csv(filename, index=False)
        earnings.to_csv(filename, index=False)

    # if Attendances file
    if str(filename).startswith("attendances"):
        attendances = pd.read_csv(args.filepath, parse_dates=['date'],usecols = ['employee_id','employer_id','date','hours','gross']).fillna("")

        # check attendances file
        compute_attendances(attendances)

        print("[*] Compute successful")

        # Output the parsed attendances file
        attendances.to_csv(filename, index=False)

    print("[*] The data has been saved to", filename)

    return

def compute_employees(employees):
    # change to datetime YYYY-MM-DD for birth_date and status_date
    employees['birth_date'] = pd.to_datetime(employees['birth_date'], format='%Y-%m-%d').dt.date
    employees['status_date'] = pd.to_datetime(employees['birth_date'], format='%Y-%m-%d').dt.date

    # employee_id
    employees['employee_id'] = employees['employee_id'].map(lambda x: str(int(x)) if x != '' else '')

    # mobile_phone
    employees['mobile_phone'] = employees['mobile_phone'].map(lambda x: str(int(x)) if x != '' else '')

    # email
    employees['email'] = employees['email'].map(lambda x: x if '@' in x else '')

    # status
    employees['status'] = employees['status'].map(lambda x: 'active' if x == '' else x).map(lambda x: x.lower())

    # employment_type
    employees['employment_type'] = employees['employment_type'].map(lambda x: x.lower())

    # pay_type
    employees['pay_type'] = employees['pay_type'].map(lambda x: x.lower())

    # region code
    employees['region_code'] = employees['region_code'].apply(lambda x: x.upper())

    schema = {
        'employee_id': {'type': 'string', 'empty': False, 'required': True},
        'employer_id': {'type': 'string', 'empty': False, 'required': True},
        'employer_name': {'type': 'string', 'empty': False, 'required': True},
        'email': {'type': 'string', 'empty': False, 'required': True},
        'mobile_phone': {'type': 'string', 'empty': False, 'minlength': 9, 'maxlength': 10, 'required': True},
        'first_name': {'type': 'string', 'empty': False, 'required': True},
        'middle_name': {'empty': True},
        'last_name': {'type': 'string', 'empty': False, 'required': True},
        'birth_date': {'type': 'date', 'empty': True},
        'last_4_ssn': {'type': 'string', 'min': 0000, 'max': 9999, 'empty': True},
        'address_line_1': {'type': 'string', 'empty': True},
        'address_line_2': {'empty': True},
        'city': {'type': 'string', 'empty': True},
        'region_code': {'type': 'string',
                        'allowed':['VIC', 'TAS', 'NT', 'QLD', 'ACT', 'SA', 'NSW', 'WA'],
                        'empty': True},
        'country_code': {'type': 'string', 'empty': True, 'minlength': 2, 'maxlength': 2},
        'postal_code': {'type': 'integer', 'min': 1000, 'max': 9999, 'empty': True},
        'status': {'type': 'string',
                   'allowed': ['active', 'terminated'],
                   'empty': False, 'required': True},
        'status_date': {'type': 'date', 'empty': False, 'required': True},
        'employment_type': {
            'allowed': ['fulltime', 'parttime', 'casual', 'subsidized'], 'empty': True, 'required':True},
        'pay_type': {'allowed': ['salary', 'hourly'], 'empty' : True, 'required':True},
        'pay_frequency': {'type': 'string', 'allowed': ['D', 'W', 'B', 'S', 'M'], 'empty': False, 'required': True},
        'pay_days': {'empty': True, 'required' : True}
    }

    employees_dict = employees.to_dict(orient='records')

    v = Validator(schema)
    v.allow_unknown = False

    #print("Employees file errors: " + names + "\n")
    for idx, record in enumerate(employees_dict):
        if not v.validate(record):
            print(f'row {idx}, employee_id: {employees.employee_id[idx]}, {v.errors}')


def compute_earnings(earnings):
    # employee_id
    earnings['employee_id'] = earnings['employee_id'].map(lambda x: str(int(x)) if x != '' else '')

    # change to datetime YYYY-MM-DD
    earnings['check_date'] = pd.to_datetime(earnings['check_date'], format='%Y-%m-%d').dt.date
    earnings['period_start_date'] = pd.to_datetime(earnings['period_start_date'], format='%Y-%m-%d').dt.date
    earnings['period_end_date'] = pd.to_datetime(earnings['period_end_date'], format='%Y-%m-%d').dt.date

    # net_amount
    earnings['net_amount'] = earnings['net_amount'].map(lambda x: round(float(x), 2))

    # deduction_amount
    earnings['deduction_amount'] = earnings['deduction_amount'].map(lambda x: float(x) if x != '' else 0.0)

    # earning_type
    earnings['earning_type'] = earnings['earning_type'].map(lambda x: x.lower())

    schema = {
        'earning_id': {'type': 'string', 'empty': False, 'required': True},
        'employee_id': {'type': 'string', 'empty': False, 'required': True},
        'employer_id': {'type': 'string', 'empty': False, 'required': True},
        'employer_name': {'type': 'string', 'empty': False, 'required': True},
        'net_amount': {'type': 'float', 'min': 0, 'empty': False, 'required': True},
        'deduction_amount': {'type': 'float', 'min': 0, 'empty': False, 'required': True},
        'check_date': {'type': 'date', 'empty': False, 'required': True},
        'period_start_date': {'type': 'date', 'empty': False, 'required': True},
        'period_end_date': {'type': 'date', 'empty': False, 'required': True},
        'earning_type': {'type': 'string', 'allowed': ['regular', 'vacation', 'supplemental'], 'empty': True,
                         'required': True}
    }

    earnings_dict = earnings.to_dict(orient='records')

    v = Validator(schema)
    v.allow_unknown = False

    # print("earnings file errors: " + names + "\n")
    for idx, record in enumerate(earnings_dict):
        if not v.validate(record):
            print(f'row {idx}, earning_id: {earnings.earning_id[idx]}, {v.errors}')

def compute_attendances(attendances):
    # change to datetime YYYY-MM-DD for date
    attendances['date'] = pd.to_datetime(attendances['date'], format='%Y-%m-%d').dt.date


    # employee_id
    attendances['employee_id'] = attendances['employee_id'].map(lambda x: str(int(x)) if x != '' else '')

    # hours
    attendances['hours'] = attendances['hours'].map(lambda x: round(float(x), 2))

    # gross
    attendances['gross'] = attendances['gross'].map(lambda x: round(float(x), 2))

    schema = {
        'employee_id': {'type': 'string', 'empty': False, 'required': True},
        'employer_id': {'type': 'string', 'empty': False, 'required': True},
        'date': {'type': 'date', 'empty': False, 'required': True},
        'hours': {'type': 'float', 'min': 0, 'empty': False, 'required': True},
        'gross': {'type': 'float', 'min': 0, 'empty': False, 'required': True}
    }

    attendances_dict = attendances.to_dict(orient='records')

    v = Validator(schema)
    v.allow_unknown = False

    #print("Attendances file errors: " + names + "\n")
    for idx, record in enumerate(attendances_dict):
        if not v.validate(record):
            print(f'row {idx}, employee_id: {attendances.employee_id[idx]}, {v.errors}')

def parse_args():
    # Function to parse arguments (Input filepath)
    parser = argparse.ArgumentParser(
        description='Compute Lattitude Employees/Earnings/Attendances File.')
    parser.add_argument("-f", "--filepath", required=False,
                        type=Path, help="Input .csv file (required)")
    args = parser.parse_args()
    create_args(args)

if __name__ == '__main__':
    sys.exit(parse_args())

# finished