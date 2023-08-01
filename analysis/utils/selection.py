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
        dataset_triggers = dataset_dict[year]["datasets"][dataset]
        overlap_triggers = []
        # All events from the n-th dataset are considered to be non-duplicates. All events from the
        #   n-1 dataset are considered to be non-duplicates if they didn't pass triggers from the
        #   n-th dataset. All events from the n-2 dataset are considered to be non-duplicates if they
        #   didn't pass triggers from either the n-1 or the n-th datasets.
        overlap_order = dataset_dict[year]["order"]
        idx = overlap_order.index(dataset)
        for dname in overlap_order[idx+1:]:
            triggers = dataset_dict[year]["datasets"][dname]
            overlap_triggers.extend(triggers)
        passes = passes_triggers(events,dataset_triggers)
        overlaps = passes_triggers(events,overlap_triggers)
    else:
        trigger_list = []
        for dname,triggers in dataset_dict[year]["datasets"].items():
            trigger_list.extend(triggers)
        passes = passes_triggers(events,trigger_list)
        overlaps = np.zero_like(passes, dtype=np.bool)  # MC samples have no overlaps

    return (passes & ~overlaps)