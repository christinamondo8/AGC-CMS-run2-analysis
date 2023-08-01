import numpy as np
import awkward as ak

dataset_dict = {
    "2018": {
        "datasets": {
            "SingleMuon" : [
                "IsoMu24",
                "IsoMu27"
            ],
        }
        "order": ["SingleMuon"]
    }
}

def passes_triggers(events,trigger_list):
    tpass = np.zeros_like(np.array(events.MET.pt), dtype=np.bool)
    trg_info_dict = events.HLT

    # "fields" should be list of all triggers in the dataset
    common_triggers = set(trg_info_dict.fields) & set(trigger_list)

    # Check to make sure that at least one of our specified triggers is present in the dataset
    if len(common_triggers) == 0 and len(trigger_list):
        raise Exception("No triggers from the sample matched to the ones used in the analysis.")

    for trg_name in common_triggers:
        tpass = tpass | trg_info_dict[trg_name]
    return tpass

def get_trigger_overlap_mask(events,dataset,is_data,year):
    # The trigger for 2016 and 2016APV are the same
    if year == "2016APV":
        year = "2016"

    if is_data:
        
    else:
        trigger_list = []
        for dname,triggers in dataset_dict[year]["datasets"].items():
            trigger_list.extend(triggers)
        passes = passes_triggers(events,trigger_list)
        overlaps = np.zero_like(passes, dtype=np.bool)  # MC samples have no overlaps

