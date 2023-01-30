from extra_data import open_run, by_id, DataCollection
from extra_data.validation import RunValidator
#from lib import boxcar
import numpy as np
import importlib
import logging
import pickle
import xarray as xr
import dask.array as da
from dask.distributed import Client, progress
from dask_jobqueue import SLURMCluster
#Lpd libraries
from extra_geom import LPD_1MGeometry
from extra_data.components import LPD1M
from pyFAI.gui import jupyter
from pyFAI.distortion import Distortion


quadPos = [[12.0, 292.0], [-5, 1.5], [261, -15], [278.5, 275]]  
lpd_path = "/gpfs/exfel/data/scratch/ardanaf/lpd"

# In case we need to shift Quadrants
qp=np.array(quadPos)
qp-=np.array([0,0])
# I load the modified version (orignal one has Q2M2 all tiles set to -100,-100 
geom=LPD_1MGeometry.from_h5_file_and_quad_positions("%s/lpd_mar_18_m2.h5" % lpd_path, qp)
pFAI_lpd=geom.to_pyfai_detector()
pFAI_lpd.aliases = ["LPD1M"]
# Modules are stack in 1-dim 256*16
pFAI_lpd.shape = (4096, 256)
pFAI_lpd.mask = np.zeros((4096, 256))
pFAI_lpd.set_pixel_corners(geom.to_distortion_array())
pFAI_lpd.distortion_correction = Distortion(pFAI_lpd,resize=True)


# Redefination of LPD1M class 
# Basically always produce a 16 module output.
# You need to 
class LPD1Mm(LPD1M):
    """ Redefination of LPD1M class. 
        always gives a 16 module output in a row (16*256, 256)
        you need to provide bad modules so they will be  set to all 0
    """
    
    def __init__(self, data: DataCollection, detector_name=None, b_modules=None,
                 *, min_modules=1, parallel_gain=False):
        modules = set(range(16))
        self._bmodules=[]
        self._gmodules=set(range(16))
        for i in b_modules:
            if i in modules:
                self._gmodules.remove(i)
                self._bmodules.append(i)
        super().__init__(data, detector_name, modules, min_modules=min_modules)
    #Reimplemnet at my tase
    def get_dask_array(self, key, subtrain_index='pulseId', fill_value=None,
                       astype=None):
        """Get a labelled Dask array of detector data
        Dask does lazy, parallelised computing, and can work with large data
        volumes. This method doesn't immediately load the data: that only
        happens once you trigger a computation.
        Parameters
        ----------
        key: str
          The data to get, e.g. 'image.data' for pixel values.
        subtrain_index: str, optional
          Specify 'pulseId' (default) or 'cellId' to label the frames recorded
          within each train. Pulse ID should allow this data to be matched with
          other devices, but depends on how the detector was manually configured
          when the data was taken. Cell ID refers to the memory cell used for
          that frame in the detector hardware.
        fill_value: int or float, optional
          Value to use for missing values. If None (default) the fill value is 0
          for integers and np.nan for floats.
        astype: Type, optional
          data type of the output array. If None (default) the dtype matches the
          input array dtype
        """
        if subtrain_index not in {'pulseId', 'cellId'}:
            raise ValueError("subtrain_index must be 'pulseId' or 'cellId'")
        arrays = []
        modnos = []
        for modno, source in sorted(self.modno_to_source.items()):
            modnos.append(modno)
            if modno in self._bmodules:
                continue
            mod_arr = self.data.get_dask_array(source, key, labelled=True)
            # At present, all the per-pulse data is stored in the 'image' key.
            # If that changes, this check will need to change as well.
            if key.startswith('image.'):
                # Add pulse IDs to create multi-level index
                inner_ix = self.data.get_array(source, 'image.' + subtrain_index)
                # Raw files have a spurious extra dimension
                if inner_ix.ndim >= 2 and inner_ix.shape[1] == 1:
                    inner_ix = inner_ix[:, 0]

                mod_arr = mod_arr.rename({'trainId': 'train_pulse'})

                mod_arr.coords['train_pulse'] = self._make_image_index(
                    mod_arr.coords['train_pulse'].values, inner_ix.values,
                    inner_name=subtrain_index,
                ).set_names('trainId', level=0)
                # This uses 'trainId' where a concrete array from the same class
                # uses 'train'. I didn't notice that inconsistency when I
                # introduced it, and now code may be relying on each name.
            if mod_arr.shape[1] == 1:
                arrays.append(mod_arr[:,0])
            else:
                arrays.append(mod_arr)
        shape=arrays[0].copy()
        for i in self._bmodules:    
            arrays.insert(i, 0*shape)
        dt = self._concat(arrays, modnos, fill_value, astype)
        dst = da.hstack(list(dt[i] for i in range(16)))

        return xr.DataArray(dst, dims=['train_pulse', 'x', 'y'], 
                            coords={'train_pulse': dt.train_pulse})

#importlib.reload(boxcar)
log = logging.getLogger(__name__)

dataDevice={"GOTTHARD1":["FXE_OGT1_SA/DAQ/DETECTOR:daqOutput", "data.adc"],
            "GOTTHARD2":["FXE_OGT3_SA/DET/RECEIVER:daqOutput", "data.adc"],
            "JFG1M2":["FXE_XAD_JF1M/DET/JNGFR02:daqOutput", "data.adc"],
            "JFG1M1":["FXE_XAD_JF1M/DET/JNGFR01:daqOutput", "data.adc"],
            "JFG500K":["FXE_XAD_JF500K/DET/JNGFR03:daqOutput", "data.adc"],
            "PINK":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_2_B.raw.samples"],
            "APD":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_2_A.raw.samples"],
            "I01":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_1_A.raw.samples"],
            "I02":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_1_B.raw.samples"],
            "I03":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_1_C.raw.samples"],
            "I04":["FXE_RR_DAQ/ADC/1:network", "digitizers.channel_1_D.raw.samples"],
           }


knobDevice={"PPODL":["FXE_AUXT_LIC/DOOCS/PPODL", "actualPosition.value"],
            "PPODL_T":["FXE_AUXT_LIC/DOOCS/PPODL", "targetPosition.value"],
            "MONO":["FXE_XTD9_MONO/MDL/ACCM_PITCH", "actualPosition.value"],
            "BAM":["FXE_AUXT_LIC/DOOCS/BAM_1932S:output", "data.lowChargeArrivalTime"]}

otherDevice={"pattern1":["SA1_RR_UTC/MDL/BUNCH_DECODER", "sase1.pulseIds.value"]}

def getWorker(scale=1, partition="exfel", reservation=None):
    # Resources per SLURM job (per node, the way SLURM is configured on Maxwell)
    # processes=16 runs 16 Dask workers in a job, so each worker has 1 core & 32 GB RAM.
    # For EuXFEL staff
    cluster = SLURMCluster(
                           queue=partition,
                           reservation=reservation,
                           local_directory='/scratch', 
                            processes=16, cores=16, memory='512GB')
    #cluster.scale(scale)
    return cluster

def getDaskWorkers(cluster):
    client = Client(cluster)
    return client


class AnalyseRun(DataCollection):
    def __init__(self, proposal, run, data_type='all'):
        mCol = open_run(proposal, run, data=data_type)
        super().__init__(mCol.files)
        self.dSet = xr.Dataset()
        
    def addData(self, name, channel, alias=None):
        if alias is None:
            alias=name
        if not(name in self.all_sources):
            print("%s is not present" % name)
            return
        if not(channel in self.keys_for_source(name)):
            print("Property %s not present for %s" % (channel, name))
            return
        self.dSet=self.dSet.merge({alias:self.get_dask_array(name, channel, labelled=True)})
        
    def addDataSet(self, data, alias):
        self.dSet = self.dSet.merge({alias:data})
        
    def addKnob(self, name, alias=None):
        if alias is None:
            alias = name
        if not(name in self.control_sources):
            print("%s is not present" % name)
            return
        nSet=xr.Dataset(data_vars={
            "%s_Trg" % alias:self.get_dask_array(name, 'targetPosition.value', labelled=True),
           "%s_Pos" % alias:self.get_dask_array(name, 'actualPosition.value', labelled=True)})
        self.dSet=self.dSet.merge(nSet)
        
    def splitOddEvenTrains(self):
        self.onSet = self.dSet.where(self.dSet.trainId % 2 ==1, drop=True)
        off = self.dSet.where(self.dSet.trainId %2, drop=True)
        offset =  off.trainId[0]-self.onSet.trainId[0]
        off = off.assign_coords(trainId=off.trainId-offset)
        self.offSet = off
    
    def splitOddEvenPulses(self, dim='pulseId'):
        self.onSet = self.dSet.where(self.dSet['%s' % dim] % 2 ==1, drop=True)
        off = self.dSet.where(self.dSet['%s' % dim] %2, drop=True)
        # off = off.assign_coords(dim:off['%s' % dim]-1)
        self.offSet = off
    
    def averageKnob(self, knob, tol=None):
        if tol is None:
            tol = np.diff(self.dSet["%s_pos" % knob]).mean()
        self.scnPts = np.unique(self.dSet["%s_Trg" % knob])
        self.subs={}
        for i in self.scnPts:
            self.subs['%s'%i]=self.dSet.where(self.dSet["%s_Trg" % knob]==i, drop=True)
        
        
        
