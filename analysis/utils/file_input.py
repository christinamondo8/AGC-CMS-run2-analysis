import json
import numpy as np
import os
from pathlib import Path
import tqdm
import urllib

# If local_data_cache is a writable path, this function will download any missing file into it and
# then return file paths corresponding to these local copies.
def construct_fileset(file_name,n_files_max_per_sample, use_xcache=False, af_name="", local_data_cache=None):
    if af_name == "ssl-dev":
        if use_xcache:
            raise RuntimeError("`use_xcache` and `af_name='ssl-dev'` are incompatible. Please only use one of them.")
        if local_data_cache is not None:
            raise RuntimeError("`af_name='ssl-dev'` and `local_data_cache` are incompatible. Please only use one of them.")

    if local_data_cache is not None:
        local_data_cache = Path(local_data_cache)
        if not local_data_cache.exists() or not os.access(local_data_cache, os.W_OK):
            raise RuntimeError(f"local_data_cache directory {local_data_cache} does not exist or is not writable.")

    # using https://atlas-groupdata.web.cern.ch/atlas-groupdata/dev/AnalysisTop/TopDataPreparation/XSection-MC15-13TeV.data
    # for reference
    # x-secs are in pb
    xsec_info = {
        "ttbar": 396.87 + 332.97, # nonallhad + allhad, keep same x-sec for all
        "single_top_s_chan": 2.0268 + 1.2676,
        "single_top_t_chan": (36.993 + 22.175)/0.252,  # scale from lepton filter to inclusive
        "single_top_tW": 37.936 + 37.906,
        "wjets": 61457 * 0.252,  # e/mu+nu final states
        "data": None,
        "SingleMuon": 1,
    }

    # For now we set CMS Run2 MC xsecs to open data xsecs -> Need to update in the future
    xsec_info["TTJets_UL2018"] = xsec_info["ttbar"]
    xsec_info["WJets_UL2018"] = xsec_info["wjets"]

    # list of files
    with open(file_name) as f:
        file_info = json.load(f)

    # process into "fileset" summarizing all info
    fileset = {}
    for process in file_info.keys():
        if process == "data":
            continue  # skip data

        if "variations" in file_info[process]:
            variations = file_info[process]["variations"]
            is_data = file_info[process]["is_data"]
            year = str(file_info[process]["year"])
            if is_data:
                process,era = process.split(f"_{year}")
            else:
                era = ""
        else:
            variations = file_info[process]
            is_data = False
            year = ""
            era = ""

        for variation in variations.keys():
            file_list = variations[variation]["files"]
            if n_files_max_per_sample != -1:
                file_list = file_list[:n_files_max_per_sample]  # use partial set of samples

            file_paths = [f["path"] for f in file_list]
            if use_xcache:
                file_paths = [f.replace("https://xrootd-local.unl.edu:1094", "root://red-xcache1.unl.edu") for f in file_paths]
            elif af_name == "ssl-dev":
                # point to local files on /data
                file_paths = [f.replace("https://xrootd-local.unl.edu:1094//store/user/", "/data/alheld/") for f in file_paths]
            if local_data_cache is not None:
                local_paths = [f.replace("https://xrootd-local.unl.edu:1094//store/user/", f"{local_data_cache.absolute()}/") for f in file_paths]
                for remote, local in zip(file_paths, local_paths):
                    if not Path(local).exists():
                        download_file(remote, local)
                file_paths = local_paths
            # Use the total number of events in entire sample
            nevts_total = variations[variation]["nevts_total"]
            metadata = {
                "process": process,
                "variation": variation,
                "nevts": nevts_total,
                "xsec": xsec_info[process],
                "is_data": is_data,
                "year": year,
                "era": era,
            }
            fileset.update({
                f"{process}{year}{era}__{variation}": {
                    "files": file_paths,
                    "metadata": metadata
                }
            })

    return fileset


def tqdm_urlretrieve_hook(t):
    """From https://github.com/tqdm/tqdm/blob/master/examples/tqdm_wget.py ."""
    last_b = [0]

    def update_to(b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] or -1,
            remains unchanged.
        """
        if tsize not in (None, -1):
            t.total = tsize
        displayed = t.update((b - last_b[0]) * bsize)
        last_b[0] = b
        return displayed

    return update_to


def download_file(url, out_file):
    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=out_path.name) as t:
        urllib.request.urlretrieve(url, out_path.absolute(), reporthook=tqdm_urlretrieve_hook(t))


