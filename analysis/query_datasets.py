import os
import json

from coffea.dataset_tools.dataset_query import DataDiscoveryCLI

# This script can be used to fully regenerate the samples json file used to get the inputs for the workbook.
#   It can also be useful if we want to change the structure of the samples json file.

class Dataset(object):
    def __init__(self,name,year,**kwargs):
        self.name = name
        self.year = year
        self.is_data = kwargs.pop('is_data',False)
        self.variation = kwargs.pop('variation','nominal')
        self.nevts_total = kwargs.pop('nevts_total',-1)
        self.xsec = kwargs.pop("xsec",-1)
        self.files = []
        self.filesets = []

    def query(self,ddc,query,**kwargs):
        metadata = {k: v for k,v in kwargs.items()}
        d = {query: metadata}
        r = ddc.load_dataset_definition(d,query_results_strategy="all",replicas_strategy="first")
        for fileset,v in r.items():
            self.filesets.append(fileset)
            for f in v['files'].keys():
                idx = f.find('/store/')
                fname = f[idx:]
                # self.files.append({"path": f"root://xcache/{fname}", "nevts": -1})
                self.files.append(f"root://xcache/{fname}")

    def asdict(self):
        d = {
            "nevts_total": self.nevts_total,
            "year": self.year,
            "files": self.files,
        }
        return d
        
if __name__ == "__main__":
    data = [
        (Dataset("DoubleMuon_A-UL2018",year="2018",nevts_total=75491789.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_B-UL2018",year="2018",nevts_total=35057758.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_C-UL2018",year="2018",nevts_total=34565869.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_D-UL2018",year="2018",nevts_total=168600679.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("EGamma_A-UL2018",year="2018",nevts_total=339013231.0,xsec=1.0,is_data=True),"/EGamma/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("EGamma_B-UL2018",year="2018",nevts_total=153792795.0,xsec=1.0,is_data=True),"/EGamma/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("EGamma_C-UL2018",year="2018",nevts_total=147827904.0,xsec=1.0,is_data=True),"/EGamma/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("EGamma_D-UL2018",year="2018",nevts_total=752524583.0,xsec=1.0,is_data=True),"/EGamma/Run2018D-UL2018_MiniAODv2_NanoAODv9-v3/NANOAOD"),
        (Dataset("MuonEG_A-UL2018",year="2018",nevts_total=32958503.0,xsec=1.0,is_data=True),"/MuonEG/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_B-UL2018",year="2018",nevts_total=16211567.0,xsec=1.0,is_data=True),"/MuonEG/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_C-UL2018",year="2018",nevts_total=15652198.0,xsec=1.0,is_data=True),"/MuonEG/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_D-UL2018",year="2018",nevts_total=71952025.0,xsec=1.0,is_data=True),"/MuonEG/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_A-UL2018",year="2018",nevts_total=241608232.0,xsec=1.0,is_data=True),"/SingleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_B-UL2018",year="2018",nevts_total=119918017.0,xsec=1.0,is_data=True),"/SingleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_C-UL2018",year="2018",nevts_total=109986009.0,xsec=1.0,is_data=True),"/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_D-UL2018",year="2018",nevts_total=513909894.0,xsec=1.0,is_data=True),"/SingleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_B-UL2017",year="2017",nevts_total=58088760.0,xsec=1.0,is_data=True),"/DoubleEG/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_C-UL2017",year="2017",nevts_total=65181125.0,xsec=1.0,is_data=True),"/DoubleEG/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_D-UL2017",year="2017",nevts_total=25911432.0,xsec=1.0,is_data=True),"/DoubleEG/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_E-UL2017",year="2017",nevts_total=56241190.0,xsec=1.0,is_data=True),"/DoubleEG/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_F-UL2017",year="2017",nevts_total=74265012.0,xsec=1.0,is_data=True),"/DoubleEG/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_B-UL2017",year="2017",nevts_total=14501767.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_C-UL2017",year="2017",nevts_total=49636525.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_D-UL2017",year="2017",nevts_total=23075733.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_E-UL2017",year="2017",nevts_total=51531477.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_F-UL2017",year="2017",nevts_total=79756560.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_B-UL2017",year="2017",nevts_total=4453465.0,xsec=1.0,is_data=True),"/MuonEG/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_C-UL2017",year="2017",nevts_total=15595214.0,xsec=1.0,is_data=True),"/MuonEG/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_D-UL2017",year="2017",nevts_total=9164365.0,xsec=1.0,is_data=True),"/MuonEG/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_E-UL2017",year="2017",nevts_total=19043421.0,xsec=1.0,is_data=True),"/MuonEG/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_F-UL2017",year="2017",nevts_total=25776363.0,xsec=1.0,is_data=True),"/MuonEG/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_B-UL2017",year="2017",nevts_total=60537490.0,xsec=1.0,is_data=True),"/SingleElectron/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_C-UL2017",year="2017",nevts_total=136637888.0,xsec=1.0,is_data=True),"/SingleElectron/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_D-UL2017",year="2017",nevts_total=51512837.0,xsec=1.0,is_data=True),"/SingleElectron/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_E-UL2017",year="2017",nevts_total=102122055.0,xsec=1.0,is_data=True),"/SingleElectron/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_F-UL2017",year="2017",nevts_total=128467223.0,xsec=1.0,is_data=True),"/SingleElectron/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_B-UL2017",year="2017",nevts_total=136300266.0,xsec=1.0,is_data=True),"/SingleMuon/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_C-UL2017",year="2017",nevts_total=165652756.0,xsec=1.0,is_data=True),"/SingleMuon/Run2017C-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_D-UL2017",year="2017",nevts_total=70361660.0,xsec=1.0,is_data=True),"/SingleMuon/Run2017D-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_E-UL2017",year="2017",nevts_total=154618774.0,xsec=1.0,is_data=True),"/SingleMuon/Run2017E-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_F-UL2017",year="2017",nevts_total=242140980.0,xsec=1.0,is_data=True),"/SingleMuon/Run2017F-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_B-ver1_HIPM_UL2016",year="2016APV",nevts_total=5686987.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleEG_B-ver2_HIPM_UL2016",year="2016APV",nevts_total=143073268.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v3/NANOAOD"),
        (Dataset("DoubleEG_C-HIPM_UL2016",year="2016APV",nevts_total=47677856.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleEG_D-HIPM_UL2016",year="2016APV",nevts_total=53324960.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleEG_E-HIPM_UL2016",year="2016APV",nevts_total=49877710.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleEG_F-HIPM_UL2016",year="2016APV",nevts_total=30216940.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleEG_F-UL2016",year="2016",nevts_total=4360689.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_G-UL2016",year="2016",nevts_total=78797031.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016G-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleEG_H-UL2016",year="2016",nevts_total=85388673.0,xsec=1.0,is_data=True),"/DoubleEG/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_B-ver1_HIPM_UL2016",year="2016APV",nevts_total=4199947.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_B-ver2_HIPM_UL2016",year="2016APV",nevts_total=82535526.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_C-HIPM_UL2016",year="2016APV",nevts_total=27934629.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_D-HIPM_UL2016",year="2016APV",nevts_total=33861745.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_E-HIPM_UL2016",year="2016APV",nevts_total=28246946.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_F-HIPM_UL2016",year="2016APV",nevts_total=17900759.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_F-UL2016",year="2016",nevts_total=2429162.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("DoubleMuon_G-UL2016",year="2016",nevts_total=45235604.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016G-UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("DoubleMuon_H-UL2016",year="2016",nevts_total=48912812.0,xsec=1.0,is_data=True),"/DoubleMuon/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_B-ver1_HIPM_UL2016",year="2016APV",nevts_total=225271.0,xsec=1.0,is_data=True),"/MuonEG/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_B-ver2_HIPM_UL2016",year="2016APV",nevts_total=32727796.0,xsec=1.0,is_data=True),"/MuonEG/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_C-HIPM_UL2016",year="2016APV",nevts_total=15405678.0,xsec=1.0,is_data=True),"/MuonEG/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_D-HIPM_UL2016",year="2016APV",nevts_total=23482352.0,xsec=1.0,is_data=True),"/MuonEG/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_E-HIPM_UL2016",year="2016APV",nevts_total=22519303.0,xsec=1.0,is_data=True),"/MuonEG/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_F-HIPM_UL2016",year="2016APV",nevts_total=14100826.0,xsec=1.0,is_data=True),"/MuonEG/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("MuonEG_F-UL2016",year="2016",nevts_total=1901339.0,xsec=1.0,is_data=True),"/MuonEG/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_G-UL2016",year="2016",nevts_total=33854612.0,xsec=1.0,is_data=True),"/MuonEG/Run2016G-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("MuonEG_H-UL2016",year="2016",nevts_total=29236516.0,xsec=1.0,is_data=True),"/MuonEG/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_B-ver1_HIPM_UL2016",year="2016APV",nevts_total=1422819.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_B-ver2_HIPM_UL2016",year="2016APV",nevts_total=246440440.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_C-HIPM_UL2016",year="2016APV",nevts_total=97259854.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_D-HIPM_UL2016",year="2016APV",nevts_total=148167727.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_E-HIPM_UL2016",year="2016APV",nevts_total=117269446.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_F-HIPM_UL2016",year="2016APV",nevts_total=61735326.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleElectron_F-UL2016",year="2016",nevts_total=8858206.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_G-UL2016",year="2016",nevts_total=153363109.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016G-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleElectron_H-UL2016",year="2016",nevts_total=129021893.0,xsec=1.0,is_data=True),"/SingleElectron/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_B-ver1_HIPM_UL2016",year="2016APV",nevts_total=2789243.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016B-ver1_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_B-ver2_HIPM_UL2016",year="2016APV",nevts_total=158145722.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016B-ver2_HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_C-HIPM_UL2016",year="2016APV",nevts_total=67441308.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016C-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_D-HIPM_UL2016",year="2016APV",nevts_total=98017996.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016D-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_E-HIPM_UL2016",year="2016APV",nevts_total=90984718.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016E-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_F-HIPM_UL2016",year="2016APV",nevts_total=57465359.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016F-HIPM_UL2016_MiniAODv2_NanoAODv9-v2/NANOAOD"),
        (Dataset("SingleMuon_F-UL2016",year="2016",nevts_total=8024195.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016F-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_G-UL2016",year="2016",nevts_total=149916849.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016G-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
        (Dataset("SingleMuon_H-UL2016",year="2016",nevts_total=174035164.0,xsec=1.0,is_data=True),"/SingleMuon/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD"),
    ]
    mc = [
        (Dataset("TTJets_UL2016APV",year="2016APV",nevts_total=93965988,xsec=831.76,is_data=False),"/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("TTJets_UL2016",year="2016",nevts_total=89003534,xsec=831.76,is_data=False),"/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("TTJets_UL2017",year="2017",nevts_total=248174388,xsec=831.76,is_data=False),"/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM"),
        (Dataset("TTJets_UL2018",year="2018",nevts_total=304895029,xsec=831.76,is_data=False),"/TTJets_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"),
        (Dataset("TTTo2L2Nu_UL2016APV",year="2016APV",nevts_total=37505000,xsec=87.31483776,is_data=False),"/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("TTTo2L2Nu_UL2016",year="2016",nevts_total=43546000,xsec=87.31483776,is_data=False),"/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("TTTo2L2Nu_UL2017",year="2017",nevts_total=106724000,xsec=87.31483776,is_data=False),"/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM"),
        (Dataset("TTTo2L2Nu_UL2018",year="2018",nevts_total=145020000,xsec=87.31483776,is_data=False),"/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"),
        (Dataset("TTToSemiLeptonic_UL2016APV",year="2016APV",nevts_total=132178000,xsec=364.35080448,is_data=False),"/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("TTToSemiLeptonic_UL2016",year="2016",nevts_total=144722000,xsec=364.35080448,is_data=False),"/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("TTToSemiLeptonic_UL2017",year="2017",nevts_total=346052000,xsec=364.35080448,is_data=False),"/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM"),
        (Dataset("TTToSemiLeptonic_UL2018",year="2018",nevts_total=476408000,xsec=364.35080448,is_data=False),"/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"),
        (Dataset("WJetsToLNu_UL2016APV",year="2016APV",nevts_total=28949426,xsec=61526.7,is_data=False),"/WJetsToLNu_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v2/NANOAODSIM"),
        (Dataset("WJetsToLNu_UL2016",year="2016",nevts_total=80958227,xsec=61526.7,is_data=False),"/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("WJetsToLNu_UL2017",year="2017",nevts_total=78307186,xsec=61526.7,is_data=False),"/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM"),
        (Dataset("WJetsToLNu_UL2018",year="2018",nevts_total=81051269,xsec=61526.7,is_data=False),"/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"),
        (Dataset("WZTo3LNu_UL2016APV",year="2016APV",nevts_total=1080000,xsec=5.284311882352942,is_data=False),"/WZTo3LNu_mllmin4p0_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("WZTo3LNu_UL2016",year="2016",nevts_total=904000,xsec=5.284311882352942,is_data=False),"/WZTo3LNu_mllmin4p0_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v2/NANOAODSIM"),
        (Dataset("WZTo3LNu_UL2017",year="2017",nevts_total=1994000,xsec=5.284311882352942,is_data=False),"/WZTo3LNu_mllmin4p0_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2/NANOAODSIM"),
        (Dataset("WZTo3LNu_UL2018",year="2018",nevts_total=1998000,xsec=5.284311882352942,is_data=False),"/WZTo3LNu_mllmin4p0_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"),
        (Dataset("WWTo2L2Nu_UL2016APV",year="2016APV",nevts_total=3018000,xsec=12.178,is_data=False),"/WWTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("WWTo2L2Nu_UL2016",year="2016",nevts_total=2900000,xsec=12.178,is_data=False),"/WWTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("WWTo2L2Nu_UL2017",year="2017",nevts_total=7098000,xsec=12.178,is_data=False),"/WWTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2/NANOAODSIM"),
        (Dataset("WWTo2L2Nu_UL2018",year="2018",nevts_total=9994000,xsec=12.178,is_data=False),"/WWTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"),
        (Dataset("ZZTo4L_UL2016APV",year="2016APV",nevts_total=49691000,xsec=1.256,is_data=False),"/ZZTo4L_TuneCP5_13TeV_powheg_pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("ZZTo4L_UL2016",year="2016",nevts_total=52104000,xsec=1.256,is_data=False),"/ZZTo4L_TuneCP5_13TeV_powheg_pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("ZZTo4L_UL2017",year="2017",nevts_total=99388000,xsec=1.256,is_data=False),"/ZZTo4L_TuneCP5_13TeV_powheg_pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2/NANOAODSIM"),
        (Dataset("ZZTo4L_UL2018",year="2018",nevts_total=98488000,xsec=1.256,is_data=False),"/ZZTo4L_TuneCP5_13TeV_powheg_pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"),
        (Dataset("DY10to50_UL2016APV",year="2016APV",nevts_total=25799525,xsec=18610.0,is_data=False),"/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("DY10to50_UL2016",year="2016",nevts_total=22388550,xsec=18610.0,is_data=False),"/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("DY10to50_UL2017",year="2017",nevts_total=68480179,xsec=18610.0,is_data=False),"/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM"),
        (Dataset("DY10to50_UL2018",year="2018",nevts_total=94452816,xsec=18610.0,is_data=False),"/DYJetsToLL_M-10to50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"),
        (Dataset("DY50_UL2016APV",year="2016APV",nevts_total=90947213,xsec=6025.2,is_data=False),"/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODAPVv9-106X_mcRun2_asymptotic_preVFP_v11-v1/NANOAODSIM"),
        (Dataset("DY50_UL2016",year="2016",nevts_total=71839442,xsec=6025.2,is_data=False),"/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM"),
        (Dataset("DY50_UL2017",year="2017",nevts_total=195529774,xsec=6025.2,is_data=False),"/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v2/NANOAODSIM"),
        (Dataset("DY50_UL2018",year="2018",nevts_total=195510810,xsec=6025.2,is_data=False),"/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"),
    ]

    samples = []
    samples += data
    samples += mc

    from_scratch = True
    
    outf_name = "json/Run2_data.json"
    if os.path.exists(outf_name) and not from_scratch:
        with open(outf_name,'r') as f:
            jsn = json.load(f)
    else:
        jsn = {}

    for ds,query in samples:
        ddc = DataDiscoveryCLI()
        ddc.do_regex_sites(r"T[123]_(US)_\w+")
        ds.query(ddc,query)
        # jsn[ds.name] = {"variations": {ds.variation: ds.asdict()}}
        jsn[ds.name] = ds.asdict()

    with open(outf_name,'w') as f:
        json.dump(jsn,f,indent=4,sort_keys=True)