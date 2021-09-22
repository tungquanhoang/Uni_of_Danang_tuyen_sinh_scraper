import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)

data = pd.read_excel("DUE_trung_tuyen.xlsx", index_col=0)

data = data.drop_duplicates()

def splitSubject(value):
    colon_pos = value.find(":")
    return value[:colon_pos]

subjects1 = data["d1"].apply(splitSubject)
subjects2 = data["d2"].apply(splitSubject)
subjects3 = data["d3"].apply(splitSubject)
subjects = pd.concat([subjects1, subjects2, subjects3]).unique()

data_copy = data.copy()

def splitSubject2(value, subject_number):
    colon_pos = value.find(":")
    subject = value[:colon_pos].strip()
    grade = value[(colon_pos + 1):]

    if subject == subjects[subject_number]:
        return float(grade)
    else:
        return np.nan


for i in range(len(subjects)):
    data_copy[subjects[i]] = data_copy["d1"].apply(splitSubject2, args=(i,))
    data_copy[subjects[i]] = data_copy[subjects[i]].fillna(data_copy["d2"].apply(splitSubject2, args=(i,)))
    data_copy[subjects[i]] = data_copy[subjects[i]].fillna(data_copy["d3"].apply(splitSubject2, args=(i,)))

data_copy.drop(['d1', 'd2', 'd3'], axis=1, inplace=True)

data_copy.to_excel('DUE_trung_tuyen_processed.xlsx')
