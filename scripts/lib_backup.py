#!/usr/bin/env python

# Original author: Stefan Saam, github@saams.de
# Performance optimizations added while maintaining original functionality

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
import lib_mail
import lib_system
from collections import defaultdict

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
        self.__setup = setup
        self.const_IMAGE_DATABASE_FILENAME = self.__setup.get_val('const_IMAGE_DATABASE_FILENAME')
        self.conf_MAIL_NOTIFICATIONS = self.__setup.get_val('conf_MAIL_NOTIFICATIONS')
        self.__conf_DISP_FRAME_TIME = self.__setup.get_val('conf_DISP_FRAME_TIME')

        self.__display = display
        self.__log = log
        self.__lan = lan
        self.FilesToProcess = FilesToProcess
        self.FilesToProcess_possible_more = FilesToProcess_possible_more
        self.SourceDevice = SourceDevice
        self.TargetDevice = TargetDevice
        self.vpn = vpn

        self.StartTime = lib_system.get_uptime_sec()
        self.StopTime = 0
        self.CountProgress = 0
        self.CountSkip = 0
        self.CountProgress_OLD = -1
        self.CountJustCopied = 0
        self.CountFilesConfirmed = 0
        self.CountFilesNotConfirmed = 0
        self.countFilesMissing = 0
        self.LastMessageTime = 0
        self.TransferRate = ''
        self.TIMSCopied = False

        self.DisplayLine1 = DisplayLine1
        self.DisplayLine2 = DisplayLine2

        # Use set instead of list for O(1) lookup
        self.FilesList = set()

        # Cache frequently accessed language strings
        self.__cached_strings = {
            'box_backup_of': self.__lan.l('box_backup_of'),
            'box_backup_checking_old_files': self.__lan.l('box_backup_checking_old_files'),
            'box_backup_time_remaining': self.__lan.l('box_backup_time_remaining')
        }

        # start screen
        self.progress(TransferMode='init', CountProgress=0)

    def progress(self, TransferMode=None, SyncOutputLine='', CountProgress=None):
        SyncOutputLine = SyncOutputLine.strip('\n')

        if CountProgress is not None:
            self.CountProgress = CountProgress

        if TransferMode == 'rsync':
            if SyncOutputLine:
                if SyncOutputLine[0] == ' ':
                    # Fast transfer info line processing
                    try:
                        self.TransferRate = SyncOutputLine.strip().split()[2]
                    except:
                        pass
                elif not any(skip in SyncOutputLine for skip in (':', '/', 'Ignoring "log file"', 'sent ', 'total size is')):
                    # Optimized file processing
                    if SyncOutputLine not in self.FilesList:
                        self.CountProgress += 1
                        self.FilesList.add(SyncOutputLine)
                        if not self.TIMSCopied:
                            self.TIMSCopied = 'tims/' in SyncOutputLine
                elif 'Number of regular files transferred:' in SyncOutputLine:
                    try:
                        self.CountJustCopied = int(SyncOutputLine.split(':')[1].strip())
                    except:
                        pass

        elif TransferMode == 'rclone':
            if SyncOutputLine:
                if SyncOutputLine[:2] == ' *' or SyncOutputLine.startswith('Transferred:'):
                    try:
                        self.TransferRate = SyncOutputLine.split(',')[-2].strip()
                    except:
                        pass
                else:
                    LineType, LineResult, FileName = self.rclone_analyse_line(SyncOutputLine)
                    if FileName not in self.FilesList:
                        if LineType == 'INFO' and 'Copied' in LineResult:
                            self.CountProgress += 1
                            self.CountJustCopied += 1
                            self.FilesList.add(FileName)
                            if not self.TIMSCopied:
                                self.TIMSCopied = 'tims/' in SyncOutputLine

        elif TransferMode == 'gphoto2':
            if SyncOutputLine.startswith(('Saving', 'Skip')):
                self.CountProgress += 1
                if SyncOutputLine.startswith('Saving'):
                    self.CountJustCopied += 1
                    self.FilesList.add(SyncOutputLine.replace('Saving file as ', ''))
                else:
                    self.CountSkip += 1

        elif TransferMode is None:
            self.CountProgress += 1

        if self.CountProgress > self.CountProgress_OLD:
            self.CountProgress_OLD = self.CountProgress
            self.__display_progress()

    def rclone_analyse_line(self, Line):
        # Optimized line parsing with fewer splits
        parts = Line.split(':')
        try:
            LineType = parts[2].split()[1].strip()
        except:
            LineType = ''

        try:
            FileName = Line.split(' : ')[1].split(':')[0].strip()
        except:
            FileName = ''

        try:
            LineResult = parts[-1].strip()
        except:
            LineResult = ''

        return LineType, LineResult, FileName

    def __display_progress(self):
        current_time = lib_system.get_uptime_sec()
        if (current_time - self.LastMessageTime >= self.__conf_DISP_FRAME_TIME or
            self.CountProgress == 0 or
            self.FilesToProcess == self.CountProgress):

            if self.TransferRate and self.TransferRate[0] != ',':
                self.TransferRate = f", {self.TransferRate}"

            DisplayLine3 = f"{self.CountProgress} {self.__cached_strings['box_backup_of']} {self.FilesToProcess}{'+'if self.FilesToProcess_possible_more else ''}{self.TransferRate}"

            # Optimized progress calculation
            if self.FilesToProcess > 0:
                if self.CountProgress > 0:
                    PercentFinished = str(round(self.CountProgress / self.FilesToProcess * 100, 1))
                    DisplayLine5 = f"PGBAR={PercentFinished}"
                else:
                    DisplayLine5 = self.__cached_strings['box_backup_checking_old_files']
            else:
                DisplayLine5 = "PGBAR=0"

            # Optimized time calculation
            if self.CountProgress > self.CountSkip:
                TimeElapsed = current_time - self.StartTime
                TimeRemaining = (self.FilesToProcess - self.CountProgress) * TimeElapsed / (self.CountProgress - self.CountSkip)
                TimeRemainingFormatted = str(timedelta(seconds=TimeRemaining)).split('.')[0]
            else:
                TimeRemainingFormatted = '?'

            DisplayLine4 = f"{self.__cached_strings['box_backup_time_remaining']}: {TimeRemainingFormatted}"

            DisplayLinesExtra = []
            if self.vpn:
                DisplayLinesExtra.append(f"s=hc:VPN: {self.vpn.check_status(10)}")

            FrameTime = self.__conf_DISP_FRAME_TIME * 1.5 if self.FilesToProcess == self.CountProgress else self.__conf_DISP_FRAME_TIME

            self.__display.message(
                [f"set:clear,time={FrameTime}",
                 f"s=hc:{self.DisplayLine1}",
                 f"s=hc:{self.DisplayLine2}",
                 f"s=hc:{DisplayLine3}",
                 f"s=hc:{DisplayLine4}",
                 f"s=hc:{DisplayLine5}"] + DisplayLinesExtra
            )

            self.LastMessageTime = current_time

class reporter(object):
    def __init__(self, lan, SourceStorageType, SourceCloudService, SourceDeviceLbbDeviceID, 
                 TargetStorageType, TargetCloudService, TargetDeviceLbbDeviceID, TransferMode, 
                 move_files, SyncLog=True):
        self.__lan = lan
        self.__SourceStorageType = SourceStorageType
        self.__SourceCloudService = SourceCloudService
        self.__SourceDeviceLbbDeviceID = SourceDeviceLbbDeviceID
        self.__TargetStorageType = TargetStorageType
        self.__TargetCloudService = TargetCloudService
        self.__TargetDeviceLbbDeviceID = TargetDeviceLbbDeviceID
        self.__TransferMode = TransferMode
        self.__move_files = move_files
        self.__SyncLog = SyncLog
        self.__Folder = None
        self.__BackupReports = {}
        self.StartTime = lib_system.get_uptime_sec()
        self.StopTime = 0

        # Cache CSS styles for HTML generation
        self.__css_styles = {
            'margins_left_1': 'margin-left:10px;margin-top:0;margin-bottom:0;',
            'font_format_alert': 'font-weight: bold; color: #ff0000;'
        }

        # Initialize shared output values
        self.mail_subject = ''
        self.mail_content_PLAIN = ''
        self.mail_content_HTML = ''
        self.display_summary = []

    def new_folder(self, Folder):
        self.__Folder = Folder if Folder else '/'
        self.__BackupReports[self.__Folder] = []

    def new_try(self):
        self.__BackupReports[self.__Folder].append({
            'FilesToProcess': 0,
            'FilesToProcess_possible_more': False,
            'FilesProcessed': 0,
            'FilesCopied': 0,
            'FilesToProcessPost': None,
            'SyncReturnCode': 0,
            'SyncLogs': [],
            'Results': [],
            'Errors': []
        })

    def set_values(self, FilesToProcess=None, FilesToProcess_possible_more=None, 
                  FilesProcessed=None, FilesCopied=None, FilesToProcessPost=None, 
                  SyncReturnCode=None):
        current_report = self.__BackupReports[self.__Folder][-1]
        
        if FilesToProcess is not None:
            current_report['FilesToProcess'] = FilesToProcess
        if FilesToProcess_possible_more is not None:
            current_report['FilesToProcess_possible_more'] = FilesToProcess_possible_more
        if FilesProcessed is not None:
            current_report['FilesProcessed'] = FilesProcessed
        if FilesCopied is not None:
            current_report['FilesCopied'] = FilesCopied
        if FilesToProcessPost is not None:
            current_report['FilesToProcessPost'] = FilesToProcessPost
        if SyncReturnCode is not None:
            current_report['SyncReturnCode'] = SyncReturnCode

    def add_synclog(self, SyncLog=''):
        if self.__SyncLog and SyncLog.strip():
            self.__BackupReports[self.__Folder][-1]['SyncLogs'].append(SyncLog.strip())

    def add_result(self, Result=''):
        self.__BackupReports[self.__Folder][-1]['Results'].append(Result)

    def add_error(self, Error=''):
        self.__BackupReports[self.__Folder][-1]['Errors'].append(Error)

    def get_errors(self):
        return self.__BackupReports[self.__Folder][-1]['Errors']

    def get_time_elapsed(self):
        if self.StopTime == 0:
            self.StopTime = lib_system.get_uptime_sec()
        TimeElapsed = self.StopTime - self.StartTime
        return str(timedelta(seconds=TimeElapsed)).split('.')[0].replace('day', 'd')

    def prepare_mail(self):
        CSS_margins_left_1 = self.__css_styles['margins_left_1']
        CSS_font_format_alert = self.__css_styles['font_format_alert']
        BackupComplete = True

        # Build mail content
        self.mail_content_HTML = f"<h2>{self.__lan.l('box_backup_mail_summary')}:</h2>"
        self.mail_content_HTML += f"\n  <b>{self.__lan.l('box_backup_mail_backup_type')}:</b>"
        self.mail_content_HTML += f"\n    <p style='{CSS_margins_left_1}'><b>{self.__lan.l(f'box_backup_mode_{self.__SourceStorageType}')} '{self.__SourceCloudService}{' ' if self.__SourceDeviceLbbDeviceID else ''}{self.__SourceDeviceLbbDeviceID}'</b> {self.__lan.l('box_backup_mail_to')} <b>{self.__lan.l(f'box_backup_mode_{self.__TargetStorageType}')} '{self.__TargetCloudService}{' ' if self.__TargetDeviceLbbDeviceID else ''}{self.__TargetDeviceLbbDeviceID}'</b> ({self.__TransferMode})</br> \
        {self.__lan.l(f'box_backup_report_time_elapsed')}: {self.get_time_elapsed()}</b></p></br>\n"

        if self.__move_files:
            self.mail_content_HTML += f"\n<p><b>{self.__lan.l('box_backup_mail_removed_source')}</b></p></br>\n"

        separator = False

        if not self.__BackupReports:
            self.new_folder('None')
            self.new_try()
            self.add_error('Err.: No backup!')

        for Folder in self.__BackupReports:
            BackupComplete = BackupComplete and (not self.__BackupReports[Folder][-1]['Errors'])

            if separator:
                self.mail_content_HTML += '\n</br>\n<hr style="width:50%;">\n</br>\n'
            separator = True

            # Add folder section
            self.mail_content_HTML += f"\n  <h3>{self.__lan.l('box_backup_folder')}: &quot;{Folder}&quot;</h3>"

            # Process tries
            tryNumber = len(self.__BackupReports[Folder])
            for Report in reversed(self
