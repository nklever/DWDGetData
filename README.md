# DWDGetData
Based on the [opendata-content.log-tool](https://github.com/DeutscherWetterdienst/opendata-content.log-tool) this is an extended python module, which can be used as single script or imported to be used in a framework.  

The main differences to the opendata-content.log-tool are
1. a single python script - the 3 external shell-commands are integrated into the script,
2. it's designed as a class, so it could be used in a python framework,
3. it could download most of the data of the opendata server of the Deutschen Wetterdienst (https://opendata.dwd.de),
4. the shell glob pattern has to be changed to a regular expression pattern,
5. compressed files (zip, bz2, gz) are automatically decompressed and returned as text-strings,
6. json files (json, geojson) are automatically returned as python json objects,
7. bufr and grib data are currently not analyzed,
8. logging could be enabled for a regularly downloading of data,
9. a path could be specified to store the downloaded data.

The python module **DWDGetData** is constructed of three classes
- **GetFile** as base class mainly for downloading and decompressing 
- **GetUpdatedFiles** as class for data with a content.log file in it's hierarchy (https://opendata.dwd.de/weather/ ...)
- **GetStaticFiles** as class for data in the climate part (https://opendata.dwd.de/climate-environment/ ...)

There are 3 examples in the "\_\_main\_\_" part of the module, which describes the usage.
1. the first example simulates the example of the opendata-content.log-tool.
2. the second example downloads the synoptic weather data in geojson format, which could be used e.g. as a regularly cron job.
3. the third example downloads the 10 minutes wind data of the climate section.

For an overview here the code of this part:

    arg_parser = argparse.ArgumentParser(description="Filters paths of a DWD Open Data content.log file "
                                                    "for entries that have been updated.")
    arg_parser.add_argument("content_log_file_name",
                            default="content.log.bz2",
                            help="The content.log file name",
                            metavar="CONTENT_LOG_FILE_NAME")
    arg_parser.add_argument("--url-base", "-b",
                            required=True,
                            help="resolve the paths taken from content.log relative to the given base URL; "
                                "put the URL of the content.log.bz2 here to end up with correct hyperlinks "
                                "to DWD's Open Data")
    arg_parser.add_argument("--pattern", "-p",
                            required=True,
                            help="regular expression pattern to be searched in content.log file")
    arg_parser.add_argument("--updated-since", "-u",
                            type=datetime.datetime.fromisoformat,
                            default=None,
                            help="last time files were checked for updates")
    arg_parser.add_argument("--min-delta", "-d",
                            default=60, type=int,
                            help="minimum number of seconds a file needs to be younger than UPDATED_SINCE (default: 60)")
    arg_parser.add_argument("--logLevel", "-l",
                            type=int,
                            choices=[0,10,20,30,40,50],
                            #choices=["NOTSET","DEBUG","INFO","WARNING","ERROR","CRITICAL"],
                            default=0,
                            help="logLevel if specified (if not, no logging enabled)")
    arg_parser.add_argument("--localStoragePath", "-s",
                            type=str,
                            default=None,
                            help="path to store original downloaded files if specified, else no storing is done")
    arg_parser.add_argument('--version', action='version', version=GetUpdatedFiles.__version__)


    example = -1 # use arguments from command line

    if example == 1:
        # Example from https://github.com/DeutscherWetterdienst/opendata-content.log-tool
        args = arg_parser.parse_args(["content.log.bz2",
                        "--url-base", "https://opendata.dwd.de/weather/nwp",
                        "--pattern", "icon-d2/grib/03/t_2m/.*_icosahedral_.*",
                        "--updated-since", "2022-08-05 00:00",
                        "--logLevel", "0",
                        "--localStoragePath", "./",
                        ])
    elif example == 2:
        # Example for regularly (e.g. by a cron job every hour) downloaded weather data
        last_run_at = (datetime.datetime.now() - datetime.timedelta(seconds=3610))
        last_run_minute = last_run_at.minute - (last_run_at.minute % 10)
        last_run_at = last_run_at.replace(minute=last_run_minute,second=0,microsecond=0).strftime("%Y-%m-%d %H:%M")

        args = arg_parser.parse_args(["content.log.bz2",
                        "--url-base", "https://opendata.dwd.de/weather/weather_reports",
                        "--pattern", "synoptic/germany/geojson/Z__C_EDZW_.*\.geojson\.gz",
                        "--updated-since", last_run_at,
                        "--logLevel", "20",
                        "--localStoragePath", "./",
                        ])
    elif example == 3:
        # Example for singular (e.g. by a manual job) downloaded e.g. climate data

        args = arg_parser.parse_args(["None",
                        "--url-base", "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/",
                        "--pattern", "10minutenwerte_wind_\d*_akt\.zip",
                        "--logLevel", "20",
                        "--localStoragePath", "/home/nik/localDisk/WindData_Archiv/DWD_Daten/10minutenwerte_wind_now_2022-08-30/",
                        ])
        # zehn_min_ff_Beschreibung_Stationen.txt
    else:
        # Example with arguments from command line
        args = arg_parser.parse_args()

    if args.updated_since:
        instance = GetUpdatedFiles(args.url_base,
                                args.content_log_file_name,
                                args.pattern,
                                localStoragePath=args.localStoragePath,
                                logLevel=args.logLevel)
        
        updated_files = list(instance.start(args.updated_since))

        for indx, fn, content in updated_files:
            mode = "w"
            if fn.endswith(".grib2"): 
                mode = "wb"
            localFn = os.path.join(args.localStoragePath,fn)
            with open(localFn,mode) as out:
                out.write(str(content))
            print(f"downloaded {indx}. file {localFn}")
    else:
        instance = GetStaticFiles(args.url_base,
                                  args.pattern,
                                  localStoragePath=args.localStoragePath,
                                  logLevel=args.logLevel)
        
        nlst_files = list(instance.start())

        for indx, fn, content in nlst_files:
            mode = "w"
            if fn.endswith(".grib2"): 
                mode = "wb"
            localFn = os.path.join(args.localStoragePath,fn)
            with open(localFn,mode) as out:
                out.write(str(content))
            print(f"downloaded {indx}. file {localFn}")
