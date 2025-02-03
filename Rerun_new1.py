from spork_framework_2020_new import multi_case_algorithm_2020
#from spork_framework_2020_loc import multi_case_algorithm_2020_loc
#from ridiculous_framework_devSPINLOCAL import multi_case_algorithm_ML1_devSPINLOCAL

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pickle
from netCDF4 import Dataset

rerun_data = np.genfromtxt('New2023CSV.csv', delimiter=',', skip_header=1, 
                           usecols=(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16))
rerun_data_str = np.genfromtxt('New2023CSV.csv', delimiter=',', skip_header=1, usecols=(0,1), dtype=str)

print(datetime.utcnow())
#for i in range(len(durations)):
#Re-run: 0, 2, 4, 6, 8, 9, 10, 11 (for z thresholds), 12
#for i in [4,13,17,25,26]:
#for i in [65,76,94,95,99,105]:
for i in [75]:
#for i in [1]:
    #print(multi_case_algorithm_ML1(150,3.25,1.5,45,50,300,25,2,2013,8,10,18,0,1.0,-0.03755074,'KAKQ',4623))
    
    #tracks_dataframe, zdroutlines, col_areas, col_lon, col_storm_lon = multi_case_algorithm_ML1_devRF(storm_relative_dirs[i],3.25,1.5,REFlevs[i],REFlev1s[i],big_storms[i],70,-1,years[i],months[i],days[i],hours[i],start_mins[i],durations[i],calibrations[i],stations[i],h_calstm[i], 240, localfolder[i], track_dis=10, GR_mins=6.0)
    #tracks_dataframe, zdroutlines, col_areas, col_lon, col_storm_lon = multi_case_algorithm_2020(rerun_data[i,12],3.25,1.5,rerun_data[i,8],rerun_data[i,9],rerun_data[i,11],70,-999,int(rerun_data[i,0]),int(rerun_data[i,3]),int(rerun_data[i,4]),int(rerun_data[i,5]),int(rerun_data[i,6]),rerun_data[i,7],rerun_data[i,10],rerun_data_str[i,1],rerun_data[i,13], 240, track_dis=10, GR_mins=4.0)
    tracks_dataframe, stamp = multi_case_algorithm_2020(3.25,1.5,rerun_data[i,8],rerun_data[i,9],rerun_data[i,11],70,
            int(rerun_data[i,14]),int(rerun_data[i,0]),int(rerun_data[i,3]),int(rerun_data[i,4]),int(rerun_data[i,5]),int(rerun_data[i,6]),rerun_data[i,7],
            rerun_data[i,10],rerun_data_str[i,1], 240, track_dis=14, GR_mins=4.0)
#tracks_dataframe, zdroutlines, col_areas, col_lon, col_storm_lon = multi_case_algorithm_2020_loc(storm_relative_dirs[i],3.25,1.5,REFlevs[i],REFlev1s[i],big_storms[i],70,-1,years[i],months[i],days[i],hours[i],start_mins[i],durations[i],calibrations[i],stations[i],h_calstm[i], localfolder[i], 240, track_dis=10, GR_mins=6.0)

    #tracks_dataframe, zdroutlines, col_areas, col_lon, col_storm_lon = multi_case_algorithm_ML1_devLOCAL(storm_relative_dirs[i],3.25,1.5,REFlevs[i],REFlev1s[i],big_storms[i],70,storm_to_tracks[i],years[i],months[i],days[i],hours[i],start_mins[i],durations[i],calibrations[i],stations[i],h_calstm[i], localfolder[i], track_dis=10)

    tracks_dataframe.to_pickle('SPORK_RERUN'+str(int(rerun_data[i,0]))+str(int(rerun_data[i,3]))+str(int(rerun_data[i,4]))+str(rerun_data_str[i,1])+'.pkl')
#    with open('NewStamps'+str(int(rerun_data[i,0]))+str(int(rerun_data[i,3]))+str(int(rerun_data[i,4]))+str(rerun_data_str[i,1])+'.pkl', 'wb') as f:
#        pickle.dump(stamp, f)
    try:
        ref = stamp[0]
        zdr = stamp[1]
        kdp = stamp[2]
        cc = stamp[3]
        rot = stamp[8]
        zdrd = stamp[9]
        nzdr = stamp[10]
        lons = stamp[4]
        lats = stamp[5]
        Times = stamp[11]
        azim = stamp[12]
        vel = stamp[13]

        REF_shaped = np.zeros((len(ref), ref[0].shape[0], ref[0].shape[1], ref[0].shape[2]))
        ZDR_shaped = np.zeros((len(ref), ref[0].shape[0], ref[0].shape[1], ref[0].shape[2]))
        KDP_shaped = np.zeros((len(ref), ref[0].shape[0], ref[0].shape[1], ref[0].shape[2]))
        CC_shaped = np.zeros((len(ref), ref[0].shape[0], ref[0].shape[1], ref[0].shape[2]))
        ROT_shaped = np.zeros((len(ref), rot[0].shape[0], rot[0].shape[1], rot[0].shape[2]))
        lon_shaped = np.zeros((len(ref), ref[0].shape[1], ref[0].shape[2]))
        lat_shaped = np.zeros((len(ref), ref[0].shape[1], ref[0].shape[2]))
        zdrd_shaped = np.zeros((len(ref), ref[0].shape[1], ref[0].shape[2]))
        nzdr_shaped = np.zeros((len(ref), ref[0].shape[1], ref[0].shape[2]))
        azim_shaped = np.zeros((len(ref), ref[0].shape[1], ref[0].shape[2]))
        VEL_shaped = np.zeros((len(ref), ref[0].shape[0], ref[0].shape[1], ref[0].shape[2]))
        Time_shaped = []
        print(REF_shaped.shape)

        for l in range(len(ref)):
            REF_shaped[l,:,:,:] = ref[l]
            ZDR_shaped[l,:,:,:] = zdr[l]
            KDP_shaped[l,:,:,:] = kdp[l]
            CC_shaped[l,:,:,:] = cc[l]
            ROT_shaped[l,:,:,:] = rot[l]
            lon_shaped[l,:,:] = lons[l]
            lat_shaped[l,:,:] = lats[l]
            zdrd_shaped[l,:,:] = zdrd[l]
            nzdr_shaped[l,:,:] = nzdr[l]
            azim_shaped[l,:,:] = azim[l]
            VEL_shaped[l,:,:,:] = vel[l]
            print('Times string', str(Times[l]))
            datetime_thingy = datetime.strptime(str(Times[l])[0:19], "%Y-%m-%d %H:%M:%S")
            ts_1980 = datetime_thingy.timestamp()
            print('Timestamp thing', ts_1980)
            Time_shaped.append(ts_1980)

        ncfile = Dataset('SPORK_NC/SPORK_RERUN'+str(int(rerun_data[i,0]))+str(int(rerun_data[i,3]))+str(int(rerun_data[i,4]))+str(rerun_data_str[i,1])+'.nc',mode='w',format='NETCDF4_CLASSIC') 

        #Save as a netCFD file
        lat_dim = ncfile.createDimension('lat', REF_shaped.shape[2])     # latitude axis
        lon_dim = ncfile.createDimension('lon', REF_shaped.shape[3])    # longitude axis
        level_dim = ncfile.createDimension('level', REF_shaped.shape[1])    # longitude axis
        time_dim = ncfile.createDimension('time', None) # unlimited axis (can be appended to).

        # Define a 4D variable to hold the data
        temp = ncfile.createVariable('REFL',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp.units = 'dBZ' # degrees Kelvin
        temp.standard_name = 'REFL_10CM' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp[:,:,:,:] = REF_shaped # Appends data along unlimited dimension

        # Define a 4D variable to hold the data
        temp12 = ncfile.createVariable('VEL',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp12.units = 'm s-1' # degrees Kelvin
        temp12.standard_name = 'VEL' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp12[:,:,:,:] = VEL_shaped # Appends data along unlimited dimension


        # Define a 4D variable to hold the data
        temp2 = ncfile.createVariable('ZDR',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp2.units = 'dB' # degrees Kelvin
        temp2.standard_name ='ZDR' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp2[:,:,:,:] = ZDR_shaped # Appends data along unlimited dimension

        # Define a 4D variable to hold the data
        temp6 = ncfile.createVariable('KDP',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp6.units = 'deg km-1' # degrees Kelvin
        temp6.standard_name ='KDP' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp6[:,:,:,:] = KDP_shaped # Appends data along unlimited dimension

        # Define a 4D variable to hold the data
        temp7 = ncfile.createVariable('CC',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp7.units = 'dB' # degrees Kelvin
        temp7.standard_name ='CC' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp7[:,:,:,:] = CC_shaped # Appends data along unlimited dimension

        # Define a 4D variable to hold the data
        temp10 = ncfile.createVariable('ROT',np.float64,('time','level','lat','lon')) # note: unlimited dimension is leftmost
        temp10.units = 's-1' # degrees Kelvin
        temp10.standard_name ='ROT' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp10[:,:,:,:] = ROT_shaped # Appends data along unlimited dimension

        # Define a 3D variable to hold the data for ZDR depth
        temp8 = ncfile.createVariable('ZDRD',np.float64,('time','lat','lon')) # note: unlimited dimension is leftmost
        temp8.standard_name = 'ZDRD' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp8[:,:,:] = zdrd_shaped # Appends data along unlimited dimension

        # Define a 3D variable to hold the data for ZDR depth
        temp9 = ncfile.createVariable('NZDR',np.float64,('time','lat','lon')) # note: unlimited dimension is leftmost
        temp9.standard_name = 'NZDR' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp9[:,:,:] = nzdr_shaped # Appends data along unlimited dimension

        # Define a 3D variable to hold the data for ZDR depth
        temp3 = ncfile.createVariable('Lons',np.float64,('time','lat','lon')) # note: unlimited dimension is leftmost
        temp3.standard_name = 'Lons' # this is a CF standard name


        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp3[:,:,:] = lon_shaped # Appends data along unlimited dimension
        
        # Define a 3D variable to hold the data for ZDR depth
        temp11 = ncfile.createVariable('azim',np.float64,('time','lat','lon')) # note: unlimited dimension is leftmost
        temp11.standard_name = 'azim' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp11[:,:,:] = azim_shaped # Appends data along unlimited dimension

        # Define a 3D variable to hold the data for ZDR depth
        temp4 = ncfile.createVariable('Lats',np.float64,('time','lat','lon')) # note: unlimited dimension is leftmost
        temp4.standard_name = 'Lats' # this is a CF standard name

        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp4[:,:,:] = lat_shaped # Appends data along unlimited dimension

        # Define a 3D variable to hold the data for ZDR depth
        temp5 = ncfile.createVariable('Times',np.float64,('time')) # note: unlimited dimension is leftmost
        temp5.standard_name = 'Times' # this is a CF standard name


        # Write the data.  This writes the whole 3D netCDF variable all at once.
        temp5[:] = Time_shaped # Appends data along unlimited dimension

    except Exception as error:
        print('no stamp', error)

print(datetime.utcnow())
