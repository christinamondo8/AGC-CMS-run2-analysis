import os
import json
import subprocess

git_dir = subprocess.check_output(["git","rev-parse","--show-toplevel"]).decode().strip()
text_files = [
    "SingleMuon_2018A", "SingleMuon_2018B", "TTJets_UL2018", "SingleMuon_2018C",
    "SingleMuon_2018D", "WJets_UL2018"
]
mc_dict = {
    "TTJets_UL2018": {
        "nevts_total": 296_610_126
    },
    "WJets_UL2018": {
        "nevts_total": 81_051_269
    }
}
d = {}

for dataset_name in text_files:
    file_path = os.path.join(git_dir,"analysis/samples",dataset_name + ".txt")
    fin = open(file_path, mode='r')
    is_mc = dataset_name in mc_dict

    if dataset_name.find("2016") >= 0: 
        year = 2016 
    elif dataset_name.find("2017") >= 0:
        year = 2017
    elif dataset_name.find("2018") >= 0:
        year = 2018
    else: 
        print("No valid year found")

    # d[dataset_name] = {
        # "variations": {},
        # "is_data": True,
    # }

    d[dataset_name] = {}
    d[dataset_name]["variations"] = {}
    d[dataset_name]["is_data"] = not is_mc 
    d[dataset_name]["year"] = year

    # d[dataset_name]["variations"] = {"nominal": {"nevts_total": -1, "files": []} }
    # d[dataset_name]["variations"]["nominal"] = {"nevts_total": -1, "files": []}

    if is_mc:
        numb_evts = mc_dict[dataset_name]["nevts_total"]
    else: 
        numb_evts = -1
    
    d[dataset_name]["variations"]["nominal"] = {"nevts_total": numb_evts, "files": []}


    for x in fin.readlines():
        x = x.strip()
            

        y = {"path" : "root://xcache/" + x, "nevts": -1}
        print(y)
        d[dataset_name]["variations"]["nominal"]["files"].append(y)
        
    print(d[dataset_name]["variations"]["nominal"]["files"])

fout = open("Run2_data.json", mode = "w")
json.dump(d,fout,indent = 4)