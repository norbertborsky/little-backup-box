#!/usr/bin/env python

# Author: Stefan Saam, github@saams.de

#######################################################################
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#######################################################################

from datetime import datetime, timedelta
import os
import threading
from queue import Queue
from shutil import copy2

import lib_mail
import lib_system

# import lib_debug
# xx=lib_debug.debug()

class progressmonitor(object):
    def __init__(self,
            setup,
            display,
            log,
            lan,
            FilesToProcess,
            FilesToProcess_possible_more=False,
            DisplayLine1='',
            DisplayLine2='',
            SourceDevice=None,
            TargetDevice=None,
            vpn=False
        ):
        self.__setup    = setup
        self.const_IMAGE_DATABASE_FILENAME            = self.__setup.get_val('const_IMAGE_DATABASE_FILENAME')
        self.conf_MAIL_NOTIFICATIONS                = self.__setup.get_val('conf_MAIL_NOTIFICATIONS')
        self.__conf_DISP_FRAME_TIME                    = self.__setup.get_val('conf_DISP_FRAME_TIME')

        self.__display                        = display    # display object
        self.__log                            = log        # log object
        self.__lan                            = lan        # language object
        self.FilesToProcess                    = FilesToProcess
        self.FilesToProcess_possible_more    = FilesToProcess_possible_more
        self.SourceDevice                    = SourceDevice
        self.TargetDevice                    = TargetDevice
        self.vpn                            = vpn

        self.StartTime                        = lib_system.get_uptime_sec()
        self.StopTime                        = 0
        self.CountProgress                    = 0
        self.CountSkip                        = 0
        self.CountProgress_OLD                = -1
        self.CountJustCopied                = 0
        self.CountFilesConfirmed            = 0
        self.CountFilesNotConfirmed            = 0
        self.countFilesMissing                = 0
        self.LastMessageTime                = 0
        self.TransferRate                    = ''
        self.TIMSCopied                        = False

        self.DisplayLine1    = DisplayLine1
        self.DisplayLine2    = DisplayLine2

        self.FilesList        = []

        # start screen
        self.progress(TransferMode='init', CountProgress=0)


    def progress(self, TransferMode=None, SyncOutputLine='', CountProgress=None):
        SyncOutputLine    = SyncOutputLine.strip('\n')

        if not CountProgress is None:
            self.CountProgress    = CountProgress

        if TransferMode == 'rsync':
            if len(SyncOutputLine) > 0:
                if SyncOutputLine[0] == ' ':
                    # transfer info line? - get transfer data
                    try:
                        self.TransferRate    = SyncOutputLine.strip().split()[2]
                    except:
                        pass

                elif (
                    (not ":" in SyncOutputLine) and
                    (SyncOutputLine[-1] != '/') and
                    (SyncOutputLine != 'Ignoring "log file" setting.') and
                    (SyncOutputLine[0:5] != 'sent ') and
                    (SyncOutputLine[0:13] != 'total size is')
                ):
                    # interpret line as file
                    if SyncOutputLine in self.FilesList:
                        return()

                    self.CountProgress        += 1

                    self.FilesList    += [SyncOutputLine]

                    if not self.TIMSCopied:
                        self.TIMSCopied    = 'tims/' in SyncOutputLine

                elif 'Number of regular files transferred:' in SyncOutputLine:
                    try:
                        self.CountJustCopied    = int(SyncOutputLine.split(':')[1].strip())
                    except:
                        pass


        elif TransferMode == 'rclone':
            if len(SyncOutputLine) > 0:
                if SyncOutputLine[:2] == ' *' or SyncOutputLine.startswith == 'Transferred:':
                    # transfer info line? - get transfer data
                    try:
                        self.TransferRate    = SyncOutputLine.split(',')[-2].strip()
                    except:
                        pass
                else:
                    # interpret line as file
                    LineType, LineResult, FileName    = self.rclone_analyse_line(SyncOutputLine)

                    if FileName in self.FilesList:
                        return()

                    if LineType=='INFO' and 'Copied' in LineResult:
                        self.CountProgress        += 1
                        self.CountJustCopied    += 1

                        self.FilesList    += [FileName]

                        if not self.TIMSCopied:
                            self.TIMSCopied    = 'tims/' in SyncOutputLine

        elif TransferMode == 'gphoto2':
            if SyncOutputLine[0:6] == 'Saving' or  SyncOutputLine[0:4] == 'Skip':
                self.CountProgress    += 1

                if SyncOutputLine[0:6] == 'Saving':
                    self.CountJustCopied    += 1
                    self.FilesList    += [SyncOutputLine.replace('Saving file as ', '')]
                elif SyncOutputLine[0:4] == 'Skip':
                    self.CountSkip        += 1

        elif TransferMode is None:
            self.CountProgress    += 1

        if self.CountProgress > self.CountProgress_OLD:
            self.CountProgress_OLD    = self.CountProgress

            self.__display_progress()

    def rclone_analyse_line(self, Line):
        # LineType
        try:
            LineType    = Line.split(':')[2].split()[1].strip()
        except:
            LineType    = ''

        # FileName
        try:
            FileName    = Line.split(' : ')[1].split(':')[0].strip()
        except:
            FileName    = ''

        # LineResult
        try:
            LineResult    = Line.split(':')[-1].strip()
        except:
            LineResult    = ''

        return(LineType, LineResult, FileName)

    def __display_progress(self):
        if (
                (lib_system.get_uptime_sec() - self.LastMessageTime >= self.__conf_DISP_FRAME_TIME) or
                (self.CountProgress == 0) or
                (self.FilesToProcess == self.CountProgress)
        ): # print changed progress

            if len(self.TransferRate) > 0 and self.TransferRate[0] != ',':
                self.TransferRate    = f", {self.TransferRate}"

            DisplayLine3    = f"{self.CountProgress} " + self.__lan.l('box_backup_of') + f" {self.FilesToProcess}{'+' if self.FilesToProcess_possible_more else ''}{self.TransferRate}"

            # calculate progress
            PercentFinished    = None
            if self.FilesToProcess > 0:
                if self.CountProgress > 0:
                    PercentFinished    = str(round(self.CountProgress / self.FilesToProcess * 100,1))
                    DisplayLine5    = f"PGBAR={PercentFinished}"
                else:
                    DisplayLine5    = self.__lan.l('box_backup_checking_old_files')

            else:
                DisplayLine5="PGBAR=0"

            # calculte remaining time
            if self.CountProgress > self.CountSkip:
                TimeElapsed        = lib_system.get_uptime_sec() - self.StartTime
                TimeRemaining    = (self.FilesToProcess - self.CountProgress) * TimeElapsed  / (self.CountProgress - self.CountSkip)
                TimeRemainingFormatted    = str(timedelta(seconds=TimeRemaining)).split('.')[0]
            else:
                TimeRemainingFormatted    = '?'

            # DisplayLine4
            DisplayLine4    = f"{self.__lan.l('box_backup_time_remaining')}: {TimeRemainingFormatted}"

            # DisplayLinesExtra
            DisplayLinesExtra    = []
            if self.vpn:
                DisplayLinesExtra.append(f"s=hc:VPN: {self.vpn.check_status(10)}")

            # FrameTime
            FrameTime    = self.__conf_DISP_FRAME_TIME
            if self.FilesToProcess == self.CountProgress:
                FrameTime    = self.__conf_DISP_FRAME_TIME * 1.5

            # Display
            self.__display.message([f"set:clear,time={FrameTime}", f"s=hc:{self.DisplayLine1}", f"s=hc:{self.DisplayLine2}", f"s=hc:{DisplayLine3}", f"s=hc:{DisplayLine4}", f"s=hc:{DisplayLine5}"] + DisplayLinesExtra)

            self.LastMessageTime=lib_system.get_uptime_sec()

# Optimized Parallel Backup Implementation
def copy_file(src, dest):
    """Copy a single file from source to destination."""
    try:
        copy2(src, dest)
    except Exception as e:
        print(f"Error copying {src} to {dest}: {e}")

def worker(queue):
    """Worker thread to handle file copying."""
    while True:
        src, dest = queue.get()
        if src is None:
            break
        copy_file(src, dest)
        queue.task_done()

def parallel_backup(src_dir, dest_dir, num_threads=4):
    """Perform a parallel backup from src_dir to dest_dir."""
    # Create destination directory if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # Initialize a queue for file paths
    queue = Queue()

    # Start worker threads
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(queue,))
        thread.start()
        threads.append(thread)

    # Walk through the source directory and enqueue files for copying
    for root, _, files in os.walk(src_dir):
        relative_path = os.path.relpath(root, src_dir)
        target_root = os.path.join(dest_dir, relative_path)

        # Ensure target directory exists
        if not os.path.exists(target_root):
            os.makedirs(target_root)

        for file in files:
            src_file = os.path.join(root, file)
            dest_file = os.path.join(target_root, file)
            queue.put((src_file, dest_file))

    # Wait for all tasks to be completed
    queue.join()

    # Stop worker threads
    for _ in threads:
        queue.put((None, None))
    for thread in threads:
        thread.join()

# Integration Example
if __name__ == "__main__":
    source_directory = "/path/to/source"
    destination_directory = "/path/to/destination"
    number_of_threads = 8  # Adjust based on your CPU and I/O capabilities

    print("Starting backup...")
    parallel_backup(source_directory, destination_directory, num_threads=number_of_threads)
    print("Backup completed.")
