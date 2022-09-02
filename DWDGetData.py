#!/usr/bin/env python3

# DWD_content.log_Tool.py

from distutils import filelist
from importlib.resources import path
import io 
import os
import sys
import re
import json
import datetime
import logging
import bz2
import gzip
from zipfile import ZipFile
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen
from urllib.error import HTTPError
from ftplib import FTP

NEWLINE = "\n"

class GetFile():

    __version__ = "3.0.0"
    textFileExt = ['.txt','.csv','.log']
    archiveFileExt = ['.bz2','.gz','.zip']

    def __init__(self,url_base='',pattern='',logLevel=None,localStoragePath=None):
        # url_base: DWD url base of content log file or weather data
        # pattern: DWD file url pattern of files to be retrieved
        # logLevel: logLevel if specified (if not, no logging enabled)
        # localStoragePath: path to store original downloaded files if specified, else no storing is done
        self.url_base = url_base
        self.pattern = pattern
        self.log = logging.getLogger(__name__)
        if logLevel:
            thisfilename,ext = os.path.splitext(os.path.basename(__file__))
            logging.basicConfig(filename=thisfilename+'.log',
                                format='%(asctime)-15s-%(levelname)s %(filename)s/%(funcName)s - %(message)s',
                                level=logLevel)
        else:
            self.log.setLevel(logging.NOTSET) 
        self.localStoragePath = localStoragePath

    def getFile(self,path,encoding="utf-8"):
        rawBasename,ext = os.path.splitext(os.path.basename(path))
        basename = os.path.join(self.url_base,os.path.dirname(self.pattern),rawBasename)
        self.log.debug(f"rawBasename={rawBasename}, basename={basename}")
        try:
            with urlopen(path) as rf:
                if rf.status == 200 or path.startswith('file'):
                    rs = rf.read()
        except HTTPError:
            (t,v,tb) = sys.exc_info()
            if self.log:
                self.log.error(f"HTTPError for {path} ({t},{v})")
            return (False, (t,v,tb))
        if self.localStoragePath and not path.startswith('file://'):
            localBasename = os.path.basename(basename)
            localFilename = os.path.join(self.localStoragePath,localBasename+ext)
            with open(localFilename,"wb") as out:
                out.write(rs)
        if ext in self.archiveFileExt:
            rs = self.decompress(rs,fmt=ext,encoding=encoding)
            basename,ext = os.path.splitext(basename)
        if ext in self.textFileExt:
            if isinstance(rs, bytes):
                try:
                    rs = rs.decode(encoding)
                except UnicodeDecodeError:
                    rs = rs.decode('iso-8859-15')
        if ext in ['.geojson','.json']:
            try:
                rs = json.loads(rs)
            except json.JSONDecodeError:
                (t,v,tb) = sys.exc_info()
                self.log.error(f"JSONDecodeError for {path} ({t},{v})")
                return (False, (t,v,tb))
        elif ext == '.bin':
            # bufr decoding - currently not implemented
            return (False, (NotImplementedError,"BUFR not implemented",None))
        elif ext.startswith('.grib'):
            # grib decoding - currently not implemented
            pass # pass to be able to store decompressed grib-Files
        return (True, rs)

    def zipdecompress(self,content):
        with io.BytesIO() as buffer:
            buffer.write(content)
            with ZipFile(buffer) as thiszip:
                files = thiszip.namelist()
                if len(files) == 1:
                    with thiszip.open(files[0]) as thisfile:
                        rs = thisfile.read()
                        return rs
                else:
                    self.log.error("more than 1 file in zip archive")

    def decompress(self,content,fmt=".bz2",encoding="utf-8"):
        function = {
            ".bz2":bz2.decompress,
            ".gz":gzip.decompress,
            ".zip":self.zipdecompress,
        }
        rs = function[fmt](content)
        if rs.startswith(b'GRIB'):
            return rs
        else:
            return rs.decode(encoding)

    def grepFromPattern(self,content):
        if getattr(self,"content_log_file_name",None):
            rs = re.findall(self.pattern+"\|\d*\|.*", content)
        else:
            rs = re.findall(self.pattern, content)
        return rs

    def getFolderContentList(self):
        url = urlparse(self.url_base)
        ftp = FTP(url.netloc)
        ftp.login()
        try:
            dirname = url.path+os.path.dirname(self.pattern)
            ftp.cwd(dirname)
        except:
            t,v,tb = sys.exc_info()
            if v.args[0] == "550 Failed to change directory.":
                self.log.error(f"ftplib.error: 550 Failed to change directory.")
            return (False, (t,v,tb))
        rs = ftp.nlst()
        return (True, rs)

class GetUpdatedFiles(GetFile):

    def __init__(self,url_base,content_log_file_name,pattern,min_delta=60,logLevel=None,localStoragePath=None):
        # url_base: DWD url base of content log file or weather data
        # pattern: DWD file url pattern of files to be retrieved
        # content_log_file_name: DWD content.log file name
        # min_delta: minimum number of seconds a file needs to be younger than 'updated_since'
        # logLevel: logLevel if specified (if not, no logging enabled)
        # localStoragePath: path to store original downloaded files if specified, else no storing is done

        self.url_base = url_base
        self.content_log_file_name = content_log_file_name
        self.pattern = pattern
        self.min_delta = min_delta
        self.log = None
        super().__init__(url_base=url_base, pattern=pattern, logLevel=logLevel, localStoragePath=localStoragePath)

    def start(self,updated_since=None):
        # updated_since: time of last run, if None today 00:00 is taken
        if not updated_since:
            self.updated_since = datetime.datetime.combine(datetime.date.today(),datetime.time())
        elif isinstance(updated_since, datetime.datetime):
            self.updated_since = updated_since
        elif isinstance(updated_since, str):
            try:
                self.updated_since = datetime.datetime.fromisoformat(updated_since)
            except:
                raise ValueError(f"'updated_since' must be in isoformat ('YYYY-MM-DD[*HH[:MM[:SS]]]')")

        url = os.path.join(self.url_base,self.content_log_file_name)
        rs, content_log = self.getFile(url)
        if not rs:
            raise BaseException(f"Error: {url}: {content_log[1]}")
        content_log_lines = content_log.split('\n')
        if self.log: 
            self.log.info(f"content_log: getFile({url}) gots {len(content_log_lines)} lines")
        content_log_lines = self.grepFromPattern(content_log)
        if self.log:
            self.log.info(f"grepFromPattern: {self.pattern} gots {len(content_log_lines)} lines")
        updated_files = self.getUpdatedData(content_log_lines)
        if self.log:
            self.log.info(f"getUpdatedData gots {len(updated_files)} files")
            self.log.debug(f"getUpdatedData: {NEWLINE}{NEWLINE.join(f for f in updated_files)}")
        for indx, updated_file in enumerate(updated_files):
            basename, ext = os.path.splitext(os.path.basename(updated_file))
            rs, content = self.getFile(updated_file)
            if rs:
                yield (indx, basename, content)

    def getUpdatedData(self,content_log_lines):
        updated_since = self.updated_since.astimezone(datetime.timezone.utc)
        updated_files = []
        for line in content_log_lines:
            # each line is of the scheme "path|size|changed_at"
            try:
                path, size, changed_at = line.strip().split("|")
                if self.log:
                    self.log.debug(f"{path}|{size}|{changed_at}")
            except ValueError:
                (t,v,tb) = sys.exc_info()
                if self.log:
                    self.log.error(f"ValueError in line {line} ({t},{v})")
            except:
                raise
            changed_at = datetime.datetime.fromisoformat(f"{changed_at}+00:00") # add UTC-Timediff
            # print paths of files that have been updated since UPDATED_SINCE
            # but require an extra MIN_DELTA seconds
            # because behind the scenes there are two separate servers answering to opendata.dwd.de
            # which might not be exactly in sync with each other
            if (changed_at - updated_since).total_seconds() > self.min_delta:
                if self.url_base:
                    updated_files.append(os.path.join(self.url_base, path))
                else:
                    updated_files(path)
        return updated_files

class GetStaticFiles(GetFile):

    def __init__(self,url_base,pattern,logLevel=None,localStoragePath=None):
        # url_base: DWD url base of content log file or weather data
        # pattern: DWD file url pattern of files to be retrieved
        # logLevel: logLevel if specified (if not, no logging enabled)
        # localStoragePath: path to store original downloaded files if specified, else no storing is done

        self.url_base = url_base
        self.pattern = pattern
        self.log = None
        super().__init__(url_base=url_base, pattern=pattern, logLevel=logLevel, localStoragePath=localStoragePath)

    def start(self):
        rs, nlst = self.getFolderContentList()
        if not rs:
            raise BaseException(f"Error: getFolderContentList: {nlst[1]}")
        self.log.debug(f"getFolderContentList: {NEWLINE}{NEWLINE.join(f for f in nlst)}")
        nlst_reduced = self.grepFromPattern("\n".join(nlst))
        for indx, path in enumerate(nlst_reduced):
            basename, ext = os.path.splitext(os.path.basename(path))
            if self.url_base:
                rs, content = self.getFile(os.path.join(self.url_base, path))
            else:
                rs, content = self.getFile(path)
            if rs:
                yield (indx, basename, content)


__all__ = ['GetUpdatedFiles','GetFile']

if __name__ == "__main__":
    import argparse

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
