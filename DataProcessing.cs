	public class DBFunctions
    {		
		public static void createViews()
        {

            using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
            {
                con.Open();
                using (SQLiteTransaction tr = con.BeginTransaction())
                {
                    using (SQLiteCommand cmd = con.CreateCommand())
                    {
                        cmd.Transaction = tr;
                        cmd.CommandText = "DROP VIEW IF EXISTS v_psmSessions;";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "DROP VIEW IF EXISTS v_adhocSessions;";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "DROP VIEW IF EXISTS v_failedPsmSessions;";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "DROP VIEW IF EXISTS v_failedAdhocSessions;";
                        cmd.ExecuteNonQuery();

                        if (DataProcessing.hasFiles)
                        {
                            cmd.CommandText = "create VIEW v_psmSessions as select [CyberArk User], [Solution Server ID], [PSM Solution], SafeName, Case When f.PolicyID IS NULL then e.PolicyID else f.PolicyID end as PolicyID, Case When f.DeviceType IS NULL then e.DeviceType else f.DeviceType end as DeviceType, TargetAccount, TargetHost, TargetUser, ConnectionComponent, Database, [Ad-Hoc Connection], StartTime, EndTime, Case When RecordingFiles > 0 then 'Yes' else 'No' end as RecordingFound, 'No' as ConnectionFailed, ErrorOccurred, DurationElapsed, Replace(Message, '[' || SessionID || ']', '') as Message, SessionID from (select (select count(RecordingFiles.FileName) from RecordingFiles where RecordingFiles.SessionID = a.SessionID) RecordingFiles, a.SessionID, a.[CyberArk User], a.[PSM ID] as [Solution Server ID], SafeName, case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'No' as 'Ad-Hoc Connection', a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, c.RequestReason As Message from (SELECT info2, SafeName, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (300) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID from logs where code in (302) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (303) and info2 != 0) c on a.SessionID = C.SessionID) left join (select AccountName, PolicyID, DeviceType from accounts) e on TargetAccount = e.AccountName left join (select PolicyID, DeviceType, sessionRecordings.SessionID as ID from sessionRecordings) f on SessionID = f.ID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_adhocSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], SafeName, Case When f.PolicyID IS NULL then e.PolicyID else f.PolicyID end as PolicyID, Case when f.DeviceType IS NULL then e.DeviceType else f.DeviceType end as DeviceType, Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'Yes' as 'Ad-Hoc Connection', a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, Case When (select count(RecordingFiles.FileName) from RecordingFiles where RecordingFiles.SessionID = a.SessionID) > 0 Then 'Yes' Else 'No' end as RecordingFound, 'No' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(c.RequestReason, '[' || a.SessionID || ']', '') As Message, a.SessionID from (SELECT info2, SafeName, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +7, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 7)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';SrcHost') - instr(info2, 'SessionID=') - 10)) as SessionID, Info1 as ID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (378) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, info1 as ID from logs where code in (380) and info2 != 0) b on a.ID = b.ID left join (Select 'Yes' as PSMDisconnectFailed, time, info1 as ID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (381) and info2 != 0) c on a.ID = C.ID left join (select Folder, AccountName, PolicyID, DeviceType from accounts) e on (substr(Folder, 6) || e.AccountName) = TargetAccount left join (select PolicyID, DeviceType, SessionID from sessionRecordings) f on a.SessionID = f.SessionID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_failedPsmSessions as select [CyberArk User], [Solution Server ID], [PSM Solution], SafeName, Case When f.PolicyID IS NULL then e.PolicyID else f.PolicyID end as PolicyID, Case When f.DeviceType IS NULL then e.DeviceType else f.DeviceType end as DeviceType, TargetAccount, TargetHost, TargetUser, ConnectionComponent, Database, [Ad-Hoc Connection], StartTime, EndTime, Null as SessionDuration, Case When RecordingFiles > 0 then 'Yes' else 'No' end as RecordingFound, 'Yes' as ConnectionFailed, ErrorOccurred, DurationElapsed, Replace(Message, '[' || SessionID || ']', '') as Message, SessionID from (select (select count(RecordingFiles.FileName) from RecordingFiles where RecordingFiles.SessionID = a.SessionID) RecordingFiles, a.SessionID, a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], SafeName, Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'No' as 'Ad-Hoc Connection', a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, a.RequestReason As Message from (SELECT info2, SafeName, RequestReason, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (301) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID from logs where code in (302) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (303) and info2 != 0) c on a.SessionID = C.SessionID) left join (select AccountName, PolicyID, DeviceType from accounts) e on TargetAccount = e.AccountName left join (select PolicyID, DeviceType, sessionRecordings.SessionID as ID from sessionRecordings) f on SessionID = f.ID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_failedAdhocSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], SafeName, Case When f.PolicyID IS NULL then e.PolicyID else f.PolicyID end as PolicyID, Case when f.DeviceType IS NULL then e.DeviceType else f.DeviceType end as DeviceType, Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'Yes' as 'Ad-Hoc Connection',  a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, NULL as SessionDuration, Case When (select count(RecordingFiles.FileName) from RecordingFiles where RecordingFiles.SessionID = a.SessionID) > 0 Then 'Yes' Else 'No' end as RecordingFound, 'Yes' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(a.RequestReason, '[' || a.SessionID || ']', '') As Message, a.SessionID  from (SELECT info2, SafeName, RequestReason, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else  substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +7, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 7)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';SrcHost') - instr(info2, 'SessionID=') - 10)) as SessionID, Info1 as ID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (379) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, info1 as ID from logs where code in (380) and info2 != 0) b on a.ID = b.ID left join (Select 'Yes' as PSMDisconnectFailed, time, info1 as ID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (381) and info2 != 0) c on a.ID = C.ID left join (select Folder, AccountName, PolicyID, DeviceType from accounts) e on (substr(Folder, 6) || e.AccountName) = TargetAccount left join (select PolicyID, DeviceType, SessionID from sessionRecordings) f on a.SessionID = f.SessionID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                        }
                        else
                        {
                            cmd.CommandText = "create VIEW v_psmSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'No' as 'Ad-Hoc Connection', a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, 'No' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(c.RequestReason, '[' || a.SessionID || ']', '') As Message, a.SessionID from (SELECT info2, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else  substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (300) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID from logs where code in (302) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (303) and info2 != 0) c on a.SessionID = C.SessionID group by starttime, endtime, erroroccurred, targetaccount;";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_adhocSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'Yes' as 'Ad-Hoc Connection',  a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, 'No' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(c.RequestReason, '[' || a.SessionID || ']', '') As Message, substr(a.SessionID, length(a.SessionID) - 35, length(a.SessionID)) as SessionID from (SELECT info2, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else  substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], replace(info1,'Root','') as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (378) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, replace(info1,'Root','') as SessionID from logs where code in (380) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, replace(info1,'Root','') as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (381) and info2 != 0) c on a.SessionID = C.SessionID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_failedPsmSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'No' as 'Ad-Hoc Connection', a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, NULL as SessionDuration, 'Yes' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(c.RequestReason, '[' || a.SessionID || ']', '') As Message, a.SessionID from (SELECT info2, RequestReason, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else  substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (301) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID from logs where code in (302) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, substr(info2, instr(info2, 'SessionID=') +10, (instr(info2, ';Src') - instr(info2, 'SessionID=') -10)) as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (303) and info2 != 0) c on a.SessionID = C.SessionID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "create VIEW v_failedAdhocSessions as select a.[CyberArk User], a.[PSM ID] as [Solution Server ID], case when InterfaceID = 'PSMApp' then 'PSM' when InterfaceID = 'PSMPApp' then 'PSM for SSH' Else 'Unknown' end as [PSM Solution], Replace(a.TargetAccount, '\\', '') as TargetAccount, case when a.TargetHost like '%;Logon%' then substr(a.TargetHost, 0, instr(a.TargetHost, ';Logon')) else a.TargetHost end as TargetHost, a.TargetUser, case when a.ConnectionComponent like '%;ConnectAs%' then substr(a.ConnectionComponent, 0, instr(a.ConnectionComponent, ';ConnectAs')) else a.ConnectionComponent end as ConnectionComponent, Database, 'Yes' as 'Ad-Hoc Connection',  a.StartTime, Case when c.time is not null then c.time else b.EndTime end as EndTime, NULL as SessionDuration, 'Yes' as ConnectionFailed, Case When c.RequestReason not like '%PSMSR169E%' and c.RequestReason != '' then 'Yes' else 'No' End as ErrorOccurred, Case When c.RequestReason like '%PSMSR169E%' Then 'Yes' Else 'No' End as DurationElapsed, Replace(c.RequestReason, '[' || a.SessionID || ']', '') As Message, substr(a.SessionID, length(a.SessionID) - 35, length(a.SessionID)) as SessionID from (SELECT info2, RequestReason, interfaceid, time as StartTime, username as [CyberArk User], replace(info1,'Root','') as TargetAccount, Case When info2 not like '%;DataBase=%' Then substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';Dst') -17) Else substr(info2, instr(info2, 'ApplicationType=') +16 , instr(info2, ';DataBase=') -17) End as ConnectionComponent, Case When info2 not like '%;DataBase=%' Then  '-' else  substr(info2, instr(info2, 'DataBase=') +9 , instr(info2, ';Dst') - instr(info2, 'DataBase=') -9) End as Database, case when info2 not like '%;managed%' then substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Pro') +- instr(info2, 'DstHost=') - 8)) else  substr(info2, instr(info2, 'DstHost=') +8, (instr(info2, ';Managed') +- instr(info2, 'DstHost=') - 8)) end as TargetHost, substr(info2, instr(info2, 'User=') +5, length(info2) - instr(info2, 'User=') -5) as TargetUser, substr(info2, instr(info2, 'PSMID=') +6, (instr(info2, ';Session') - instr(info2, 'PSMID=') - 6)) as [PSM ID], replace(info1,'Root','') as SessionID, substr(info2, instr(info2, 'SrcHost=') +8, (instr(info2, ';User') - instr(info2, 'SrcHost=') -8)) as [PSM Host], Null as SessionDuration from logs where code in (379) and info2 != 0 and info2 like '%;PSMID%') a left join (select time as EndTime, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, replace(info1,'Root','') as SessionID from logs where code in (380) and info2 != 0) b on a.SessionID = b.SessionID left join (Select 'Yes' as PSMDisconnectFailed, time, replace(info1,'Root','') as SessionID, case when info2 not like '%;ManagedAccount=%' then substr(info2, instr(info2, 'SessionDuration=') +16, (instr(info2, ';SessionID') - instr(info2, 'SessionDuration=') - 16)) else Null end as SessionDuration, RequestReason from logs where code in (381) and info2 != 0) c on a.SessionID = C.SessionID group by starttime, endtime, erroroccurred, targetaccount";
                            cmd.ExecuteNonQuery();
                        }

                        cmd.CommandText = "ANALYZE;";
                        cmd.ExecuteNonQuery();
                        cmd.CommandText = "DROP TABLE IF EXISTS Sessions;";
                        cmd.ExecuteNonQuery();
                    }
                    tr.Commit();
                }
                con.Close();
            }
        }

		
        public static void exportToCSVFiles(bool interactiveMode = false)
        {
            try
            {
                csvFileExportIsRunning = true;
                settings = MySettings.Load();

                if (!interactiveMode)
                {
                    Console.WriteLine(DateTime.Now + " Exporting processed data to CSV files...");
                }

                string fileName = string.Empty;
                string folder = settings.CSVExportFolder;
                string exportDateTime = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-dd_hh-mm-ss");
                int index = 0;

                StringBuilder sb = new StringBuilder();

                if (settings.CreateSubfoldersWithDatetimeOfCSVExport)
                {
                    folder = Path.Combine(folder, exportDateTime);
                    if (!Directory.Exists(folder))
                    {
                        Directory.CreateDirectory(folder);
                    }
                }

                foreach (DataSet dataSet in DataProcessing.processedData)
                {
                    foreach (DataTable table in dataSet.Tables)
                    {
                        if ((settings.CSVTablesToExport[0] == -1 || settings.CSVTablesToExport.Contains(index)) && table.Rows.Count > 0)
                        {
                            fileName = table.TableName;

                            if (settings.AddDateToCSVFileName == 1)
                            {
                                fileName = exportDateTime + "_" + fileName;
                            }
                            else if (settings.AddDateToCSVFileName == 2)
                            {
                                fileName = fileName + "_" + exportDateTime;
                            }
                            fileName = folder + @"\" + fileName + ".csv";

                            using (FileStream fs = new FileStream(fileName, FileMode.Create))
                            {
                                using (StreamWriter writer = new StreamWriter(fs, Encoding.GetEncoding(settings.CSVFileEncoding)))
                                {
                                    if (settings.IncludeHeaderLine)
                                    {
                                        IEnumerable<string> columnNames = table.Columns.Cast<DataColumn>().Select(column => string.Concat("\"", column.ColumnName.ToString().Replace("\"", "\"\""), "\""));
                                        sb.Append(string.Join(settings.csvDelimiter, columnNames));
                                        writer.WriteLine(sb.ToString());
                                        sb.Clear();
                                    }

                                    foreach (DataRow row in table.Rows)
                                    {
                                        IEnumerable<string> fields = row.ItemArray.Select(field => string.Concat("\"", field.ToString().Replace("\"", "\"\""), "\""));
                                        sb.Append(string.Join(settings.csvDelimiter, fields));
                                        writer.WriteLine(sb.ToString());
                                        sb.Clear();
                                    }

                                }
                            }
                        }
                        index += 1;
                    }
                }
                if (!interactiveMode)
                {
                    Console.WriteLine(DateTime.Now + " Finished CSV files export to folder " + folder);
                }
                else
                {
                    MessageBox.Show("Finished CSV files export to folder: " + Environment.NewLine + folder, "Export finished", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            catch (Exception ex)
            {
                if (!interactiveMode)
                {
                    Console.WriteLine("An error occured during CSV file export procedure.");
                    Console.WriteLine(DateTime.Now + " " + ex.Message);
                }
                else
                {
                    MessageBox.Show("An error occured during CSV file export procedure" + Environment.NewLine + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }

            }
            finally
            {
                csvFileExportIsRunning = false;
            }
        }

        public static void exportToSqlFile(bool interactiveMode = false)
        {
            try
            {

                sqlFileExportIsRunning = true;

                settings = MySettings.Load();

                if (!interactiveMode)
                {
                    if (!settings.ExportTablesInIndividualFiles)
                    {
                        Console.WriteLine(DateTime.Now + " Exporting processed data to SQL-commands file...");
                    }
                    else if (settings.ExportTablesInIndividualFiles)
                    {
                        Console.WriteLine(DateTime.Now + " Exporting processed data to SQL-commands files...");
                    }
                }


                string singleFileName = string.Empty;
                string singleFileFolder = string.Empty;
                string multiFilesFolder = string.Empty;
                int index = 0;
                string exportFileFullPath = settings.SqlExportFile;
                string exportDateTime = Convert.ToDateTime(DateTime.Now).ToString("yyyy-MM-dd_hh-mm-ss");

                if (!settings.ExportTablesInIndividualFiles)
                {
                    singleFileName = Path.GetFileName(settings.SqlExportFile);
                    singleFileFolder = Path.GetDirectoryName(settings.SqlExportFile);

                    if (settings.AddDateToSqlFileName == 1)
                    {
                        if (Path.GetExtension(singleFileName).Length > 0)
                        {
                            singleFileName = exportDateTime + "_" + singleFileName;
                        }
                        else
                        {
                            singleFileName = exportDateTime + "_" + singleFileName + ".sql";
                        }
                    }
                    else if (settings.AddDateToSqlFileName == 2)
                    {
                        if (Path.GetExtension(singleFileName).Length > 0)
                        {
                            singleFileName = singleFileName.Replace(Path.GetExtension(singleFileName), "") + "_" + exportDateTime + ".sql";
                        }
                        else
                        {
                            singleFileName = singleFileName + "_" + exportDateTime + ".sql";
                        }
                    }

                    if (Path.GetExtension(singleFileName).Length == 0)
                    {
                        singleFileName = singleFileName + ".sql";
                    }
                }
                else if (settings.ExportTablesInIndividualFiles)
                {
                    multiFilesFolder = settings.SqlExportFolder;
                    exportFileFullPath = multiFilesFolder;
                }

                if (settings.CreateSubfoldersWithDatetimeOfExport)
                {
                    if (!settings.ExportTablesInIndividualFiles)
                    {
                        Directory.CreateDirectory(Path.Combine(singleFileFolder, exportDateTime));
                        exportFileFullPath = Path.Combine(singleFileFolder, exportDateTime) + @"\" + singleFileName;
                    }
                    else if (settings.ExportTablesInIndividualFiles)
                    {
                        Directory.CreateDirectory(Path.Combine(multiFilesFolder, exportDateTime));
                        exportFileFullPath = Path.Combine(multiFilesFolder, exportDateTime);
                    }
                }

                if (!settings.ExportTablesInIndividualFiles)
                {
                    using (FileStream fs = new FileStream(exportFileFullPath, FileMode.Create))
                    {
                        using (StreamWriter writer = new StreamWriter(fs, Encoding.GetEncoding(settings.SqlFileEncoding)))
                        {
                            writer.WriteLine("--");
                            writer.WriteLine("-- File created with " + MainWindow.PASReporterVersionLong + " on " + DateTime.Now);
                            writer.WriteLine("--");
                            writer.WriteLine("-- Text encoding used: " + settings.SqlFileEncoding);
                            writer.WriteLine("--");
                            writer.WriteLine("");
                            writer.WriteLine("");

                            foreach (DataSet dataSet in DataProcessing.processedData)
                            {
                                foreach (DataTable table in dataSet.Tables)
                                {

                                    if ((settings.SqlTablesToExport[0] == -1 || settings.SqlTablesToExport.Contains(index)) && table.Rows.Count > 0)
                                    {
                                        writer.WriteLine("-- Table " + table.TableName);

                                        if (settings.UseBatches == 2)
                                        {
                                            writer.WriteLine("BEGIN TRANSACTION;");
                                        }

                                        if (settings.AddDrobTableStatements)
                                        {
                                            writer.WriteLine("DROP TABLE IF EXISTS " + table.TableName + ";");
                                        }

                                        if (settings.AddCreateTableStatements)
                                        {
                                            string createTableQuery = "CREATE TABLE " + table.TableName + " (";

                                            string type = String.Empty;

                                            foreach (DataColumn col in table.Columns)
                                            {
                                                if (col.DataType == typeof(string))
                                                {
                                                    type = "VARCHAR(600)";
                                                }
                                                else if (col.DataType == typeof(int))
                                                {
                                                    type = "INTEGER";
                                                }
                                                else if (col.DataType == typeof(double))
                                                {
                                                    type = "REAL";
                                                }
                                                else if (col.DataType == typeof(DateTime))
                                                {
                                                    type = "DATETIME";
                                                }
                                                else
                                                {
                                                    type = "VARCHAR(600)";
                                                }

                                                createTableQuery = createTableQuery + " [" + col.ColumnName + "] " + type + ",";
                                            }

                                            createTableQuery = createTableQuery.Remove(createTableQuery.Length - 1, 1) + ");";
                                            writer.WriteLine(createTableQuery);
                                        }

                                        string insertIntoTableQuery = "INSERT INTO " + table.TableName + " VALUES (";
                                        string value = string.Empty;


                                        for (int i = 0; i < table.Rows.Count; i++)
                                        {
                                            insertIntoTableQuery = "INSERT INTO " + table.TableName + " VALUES (";
                                            for (int j = 0; j < table.Columns.Count; j++)
                                            {
                                                value = table.Rows[i][j].ToString().Replace("'", "''");
                                                if (value != "NaN")
                                                {
                                                    insertIntoTableQuery = insertIntoTableQuery + "'" + value + "',";
                                                }
                                                else
                                                {
                                                    insertIntoTableQuery = insertIntoTableQuery + "'',";
                                                }
                                            }
                                            insertIntoTableQuery = insertIntoTableQuery.Remove(insertIntoTableQuery.Length - 1, 1) + ");";
                                            writer.WriteLine(insertIntoTableQuery);
                                        }

                                        if (settings.UseBatches == 1)
                                        {
                                            writer.WriteLine("GO");
                                        }
                                        else if (settings.UseBatches == 2)
                                        {
                                            writer.WriteLine("COMMIT TRANSACTION;");
                                        }
                                        writer.WriteLine("");
                                    }
                                    index += 1;
                                }
                            }


                        }
                    }
                }
                else if (settings.ExportTablesInIndividualFiles)
                {
                    foreach (DataSet dataSet in DataProcessing.processedData)
                    {
                        foreach (DataTable table in dataSet.Tables)
                        {

                            if ((settings.SqlTablesToExport[0] == -1 || settings.SqlTablesToExport.Contains(index)) && table.Rows.Count > 0)
                            {
                                if (settings.AddDateToSqlFileName == 1)
                                {
                                    writeToSqlSingleCommandsFile(table, exportFileFullPath + @"\" + exportDateTime + "_" + table.TableName + ".sql");
                                }
                                else if (settings.AddDateToSqlFileName == 2)
                                {
                                    writeToSqlSingleCommandsFile(table, exportFileFullPath + @"\" + table.TableName + "_" + exportDateTime + ".sql");

                                }
                                else
                                {
                                    writeToSqlSingleCommandsFile(table, exportFileFullPath + @"\" + table.TableName + ".sql");
                                }
                            }
                            index += 1;
                        }
                    }
                }

                if (!settings.ExportTablesInIndividualFiles)
                {
                    if (!interactiveMode)
                    {
                        Console.WriteLine(DateTime.Now + " Finished exporting processed data to SQL-commands file");
                    }
                    else
                    {
                        MessageBox.Show("Finished exporting processed data to SQL-commands file:" + Environment.NewLine + settings.SqlExportFile, "Export completed", MessageBoxButton.OK, MessageBoxImage.Information);
                    }
                }
                else if (settings.ExportTablesInIndividualFiles)
                {
                    if (!interactiveMode)
                    {
                        Console.WriteLine(DateTime.Now + " Finished exporting processed data to SQL-commands files");
                    }
                    else
                    {
                        MessageBox.Show("Finished exporting processed data to SQL-commands files to folder:" + Environment.NewLine + settings.SqlExportFolder, "Export completed", MessageBoxButton.OK, MessageBoxImage.Information);
                    }
                }
            }

            catch (Exception ex)
            {
                if (!interactiveMode)
                {
                    Console.WriteLine("An error occured during SQL export procedure.");
                    Console.WriteLine(DateTime.Now + " " + ex.Message);
                }
                else
                {
                    MessageBox.Show("An error occured during SQL export procedure:" + Environment.NewLine + ex.Message, "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                }

            }
            finally
            {
                sqlFileExportIsRunning = false;
            }
        }

        private static void writeToSqlSingleCommandsFile(DataTable table, string filename)
        {
            using (FileStream fs = new FileStream(filename, FileMode.Create))
            {
                using (StreamWriter writer = new StreamWriter(fs, Encoding.GetEncoding(settings.SqlFileEncoding)))
                {
                    writer.WriteLine("--");
                    writer.WriteLine("-- File created with " + MainWindow.PASReporterVersionLong + " on " + DateTime.Now);
                    writer.WriteLine("--");
                    writer.WriteLine("-- Text encoding used: " + settings.SqlFileEncoding);
                    writer.WriteLine("--");
                    writer.WriteLine("");
                    writer.WriteLine("");

                    writer.WriteLine("-- Table " + table.TableName);

                    if (settings.UseBatches == 2)
                    {
                        writer.WriteLine("BEGIN TRANSACTION;");
                    }
                    if (settings.AddDrobTableStatements)
                    {
                        writer.WriteLine("DROP TABLE IF EXISTS " + table.TableName + ";");
                    }

                    if (settings.AddCreateTableStatements)
                    {
                        string createTableQuery = "CREATE TABLE " + table.TableName + " (";
                        string type = String.Empty;

                        foreach (DataColumn col in table.Columns)
                        {
                            if (col.DataType == typeof(string))
                            {
                                type = "VARCHAR(600)";
                            }
                            else if (col.DataType == typeof(int))
                            {
                                type = "INTEGER";
                            }
                            else if (col.DataType == typeof(double))
                            {
                                type = "REAL";
                            }
                            else if (col.DataType == typeof(DateTime))
                            {
                                type = "DATETIME";
                            }
                            else
                            {
                                type = "VARCHAR(600)";
                            }

                            createTableQuery = createTableQuery + " [" + col.ColumnName + "] " + type + ",";
                        }

                        createTableQuery = createTableQuery.Remove(createTableQuery.Length - 1, 1) + ");";
                        writer.WriteLine(createTableQuery);
                    }

                    string insertIntoTableQuery = "INSERT INTO " + table.TableName + " VALUES (";
                    string value = string.Empty;

                    for (int i = 0; i < table.Rows.Count; i++)
                    {
                        insertIntoTableQuery = "INSERT INTO " + table.TableName + " VALUES (";
                        for (int j = 0; j < table.Columns.Count; j++)
                        {
                            value = table.Rows[i][j].ToString().Replace("'", "''");
                            if (value != "NaN")
                            {
                                insertIntoTableQuery = insertIntoTableQuery + "'" + value + "',";
                            }
                            else
                            {
                                insertIntoTableQuery = insertIntoTableQuery + "'" + null + "',";
                            }
                        }
                        insertIntoTableQuery = insertIntoTableQuery.Remove(insertIntoTableQuery.Length - 1, 1) + ");";
                        writer.WriteLine(insertIntoTableQuery);
                    }

                    if (settings.UseBatches == 1)
                    {
                        writer.WriteLine("GO");

                    }
                    else if (settings.UseBatches == 2)
                    {
                        writer.WriteLine("COMMIT TRANSACTION;");
                    }
                }
            }
        }
	}
	
	
	
    // Class for creating the environment reports
    public class Report
    {
        public static bool reportIsGettingGenerated = false;
        static XtraReport environmentReport = new XtraReport();
        static XRPanel xRPanel = new XRPanel();
        static XRChart xRChart = new XRChart();
        static XRTable xRTable = new XRTable();
        static XRLabel headerLabel = new XRLabel();
        static XRLabel subHeaderLabel = new XRLabel();
        static DetailBand detailBand = new DetailBand();
        static DetailReportBand detailReportBand = new DetailReportBand();
        static string chartTitle = string.Empty;
        static MySettings settings;

        public int ID { get; set; }
        public string name { get; set; }
        public string displayName { get; set; }
        public string description { get; set; }

        public string documentType { get; set; }

        public Report()
        {

        }

        public Report(int ID, string name, string displayName, string description)
        {
            this.ID = ID;
            this.name = name;
            this.displayName = displayName;
            this.description = description;
        }

        public static XtraReport createReportFromID(int reportID)
        {
            if (reportID == 0)
            {
                return createEnvironmentReport();
            }
            else if (reportID == 1)
            {
                return createEnvironmentReport(1);
            }
            else if (reportID == 2)
            {
                return createEnvironmentReport(2);
            }
            else
            {
                return null;
            }
        }

        public static XtraReport createDummyReport()
        {
            XtraReport DummyReport = new XtraReport();
            try
            {

                DummyReport = new XtraReport()
                {
                    Name = "Dummy_Report",
                    DisplayName = "Process EVD export and configuration files",
                    PaperKind = PaperKind.Letter,
                    Margins = new Margins(75, 75, 49, 49),
                    Font = new Font("Calibri", 11)
                };

                DetailBand detailBand = new DetailBand()
                {
                    HeightF = 800
                };

                DummyReport.Bands.Add(detailBand);

                detailBand.SubBands.Add(addMarginSubBand(20));

                XRPictureBox informationPictureBox = new XRPictureBox();
                informationPictureBox.BeforePrint += XRControl_BeforePrint;
                informationPictureBox.ImageSource = new ImageSource((new SvgBitmap(DevExpress.Images.ImageResourceCache.Default.GetSvgImageById("Actions_Info"))).Render(null, 3.0));
                informationPictureBox.AnchorHorizontal = HorizontalAnchorStyles.Both;
                informationPictureBox.AnchorVertical = VerticalAnchorStyles.Both;
                informationPictureBox.Sizing = ImageSizeMode.Squeeze;
                informationPictureBox.HeightF = 120;
                detailBand.SubBands.Add(addControlToSubBand(informationPictureBox));

                detailBand.SubBands.Add(addMarginSubBand(10));


                XRLabel xRLabel = new XRLabel();
                xRLabel.BeforePrint += XRControl_BeforePrint;
                xRLabel.TextAlignment = TextAlignment.MiddleCenter;
                xRLabel.Text = "Please process EVD exports and configuration files so that reports can be seen here";
                xRLabel.Font = new Font("Calibri", 24, FontStyle.Bold);
                detailBand.SubBands.Add(addControlToSubBand(xRLabel));

            }
            catch
            {
                return null;
            }

            return DummyReport;
        }
		
        public static XtraReport createEnvironmentReport(int obfuscationLevel = 0)
        {
            settings = MySettings.Load();
            reportIsGettingGenerated = true;

            XtraReport environmentReport = new XtraReport()
            {
                Name = "EnvironmentReport",
                DisplayName = "Environment Report",
                PaperKind = PaperKind.Letter,
                Margins = new Margins(75, 75, 49, 49),
                Font = new Font("Calibri", 11)
            };

            if (obfuscationLevel == 1)
            {
                environmentReport.DisplayName += " (normal obfuscation)";
            }
            else if (obfuscationLevel == 2)
            {
                environmentReport.DisplayName += " (high obfuscation)";
            }

            try
            {

                ReportHeaderBand reportHeaderBand = new ReportHeaderBand
                {
                    HeightF = 50
                };
                TopMarginBand topMarginBand = new TopMarginBand()
                {
                    HeightF = 80
                };
                DetailBand detailBand = new DetailBand()
                {
                    HeightF = 950
                };
                BottomMarginBand bottomMarginBand = new BottomMarginBand()
                {
                    HeightF = 70
                };



                environmentReport.Bands.Add(topMarginBand);
                environmentReport.Bands.Add(reportHeaderBand);
                environmentReport.Bands.Add(detailBand);
                environmentReport.Bands.Add(bottomMarginBand);

                if (isNotNullorEmpty(DataProcessing.reportInformation))
                {
                    try
                    {

                        if (obfuscationLevel > 0)
                        {
                            XRLabel obfuscationLevelLabel = new XRLabel();
                            obfuscationLevelLabel.Padding = new PaddingInfo(0, 0, 0, 0);
                            obfuscationLevelLabel.TextAlignment = TextAlignment.MiddleCenter;
                            obfuscationLevelLabel.HeightF = 20;
                            obfuscationLevelLabel.Font = new Font("Calibri", 12, FontStyle.Bold);
                            obfuscationLevelLabel.CanPublish = false;
                            obfuscationLevelLabel.PrintOnPage += XRControl_OnlyPrintOnFirstPage;

                            if (obfuscationLevel == 1)
                            {
                                environmentReport.Tag = "1";
                                obfuscationLevelLabel.Text = "- Obfuscated- ";
                                environmentReport.Name += "_obfuscated";
                            }
                            else if (obfuscationLevel == 2)
                            {
                                environmentReport.Tag = "2";
                                obfuscationLevelLabel.Text = "- Highly obfuscated- ";
                                environmentReport.Name += "_HighlyObfuscated";
                            }
                            topMarginBand.Controls.Add(obfuscationLevelLabel);
                            obfuscationLevelLabel.BeforePrint += XRControl_BeforePrint;
                        }
                        else
                        {
                            environmentReport.Tag = "0";
                        }

                        XRPictureBox coverPictureBand = new XRPictureBox();
                        coverPictureBand.BeforePrint += XRControl_BeforePrint;
                        coverPictureBand.ImageSource = new ImageSource(new Bitmap(@"img\reportCoverBand.png"));
                        coverPictureBand.Sizing = ImageSizeMode.StretchImage;
                        coverPictureBand.HeightF = 100;
                        coverPictureBand.AnchorHorizontal = HorizontalAnchorStyles.Left;
                        coverPictureBand.AnchorVertical = VerticalAnchorStyles.Top;
                        SubBand coverBannerSubBand = new SubBand();
                        reportHeaderBand.SubBands.Add(coverBannerSubBand);
                        coverBannerSubBand.Controls.Add(coverPictureBand);

                        SubBand coverSubBand = new SubBand();
                        coverSubBand.PageBreak = PageBreak.AfterBand;
                        reportHeaderBand.SubBands.Add(coverSubBand);

                        XRTable coverPageTable = new XRTable();
                        coverSubBand.Controls.Add(coverPageTable);
                        coverPageTable.BeforePrint += XRControl_BeforePrint;
                        XRTableRow tableRow = new XRTableRow();
                        XRTableCell tableCell = new XRTableCell();


                        XRLabel titleLabel = new XRLabel();
                        titleLabel.Multiline = true;
                        titleLabel.Padding = new PaddingInfo(0, 0, 100, 65);
                        titleLabel.Text = "CyberArk Privileged Access Security" + Environment.NewLine + "Environment Report";
                        titleLabel.Font = new Font("Calibri", 26, FontStyle.Bold);
                        titleLabel.ForeColor = ColorTranslator.FromHtml("#2F5496");
                        titleLabel.WidthF = getReportWidthWithoutMargins();
                        titleLabel.TextAlignment = TextAlignment.MiddleCenter;
                        titleLabel.BeforePrint += XRControl_BeforePrint;
                        tableCell = new XRTableCell();
                        tableRow = new XRTableRow();
                        coverPageTable.Rows.Add(tableRow);
                        tableCell = new XRTableCell();
                        tableRow.Cells.Add(tableCell);
                        tableCell.Controls.Add(titleLabel);



                        if (isNotNullorEmpty(DataProcessing.installedVersions) && DataProcessing.installedVersions.Columns.Contains("Component") && DataProcessing.installedVersions.Columns.Contains("Version"))
                        {
                            string vaultVersion = DataProcessing.installedVersions.AsEnumerable().Where(p => p.Field<string>("Component") == "Vault").Select(p => p.Field<string>("Version")).FirstOrDefault();
                            if (vaultVersion != null && vaultVersion.Trim() != string.Empty)
                            {
                                if (vaultVersion.Split('.').Length > 1)
                                {
                                    if (vaultVersion.Split('.')[1].Length > 2)
                                    {
                                        vaultVersion = vaultVersion.Split('.')[0] + "." + vaultVersion.Split('.')[1].Substring(0, 2);
                                    }
                                    else
                                    {
                                        vaultVersion = vaultVersion.Split('.')[0] + "." + vaultVersion.Split('.')[1];
                                    }
                                }

                                tableCell = new XRTableCell();
                                tableCell.Text = "Version v" + vaultVersion;
                                tableCell.TextAlignment = TextAlignment.MiddleCenter;
                                tableCell.Padding = new PaddingInfo(0, 0, 0, 65);
                                tableCell.Font = new Font("Calibri", 22, FontStyle.Bold);
                                tableCell.ForeColor = ColorTranslator.FromHtml("#2F5496");
                                tableRow = new XRTableRow();
                                tableRow.Cells.Add(tableCell);
                                coverPageTable.Rows.Add(tableRow);
                            }
                        }

                        if (settings.reportLogoOption == 0 || settings.reportLogoOption == 1)
                        {
                            string logoLocation = string.Empty;

                            if (settings.reportLogoOption == 0 || (obfuscationLevel > 0 && settings.coverLogoObfuscationSetting == 2))
                            {
                                logoLocation = @"img\reportsCover.png";
                            }
                            else if (settings.reportLogoOption == 1)
                            {
                                logoLocation = settings.customCoverPageLogo;
                            }

                            if (File.Exists(logoLocation))
                            {
                                try
                                {
                                    if (obfuscationLevel == 0 || settings.coverLogoObfuscationSetting != 1)
                                    {
                                        XRPictureBox xRPictureBox = new XRPictureBox();
                                        xRPictureBox.BeforePrint += XRControl_BeforePrint;
                                        xRPictureBox.ImageSource = new ImageSource(new Bitmap(logoLocation));
                                        xRPictureBox.Sizing = ImageSizeMode.Squeeze;
                                        xRPictureBox.HeightF = 200;
                                        tableCell.TextAlignment = TextAlignment.MiddleCenter;
                                        tableCell = new XRTableCell();
                                        tableRow = new XRTableRow();
                                        tableRow.Padding = new PaddingInfo(0, 0, 0, 50);
                                        tableRow.Cells.Add(tableCell);
                                        coverPageTable.Rows.Add(tableRow);
                                        tableCell.Controls.Add(xRPictureBox);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("An error occurred while trying to add company logo to report cover page");
                                    Console.WriteLine(ex.Message);
                                    Console.WriteLine(ex.InnerException.Message);
                                }
                            }
                            else
                            {
                                Console.WriteLine("Error: Report cover logo could not be found");
                                Console.WriteLine("File path: " + logoLocation + Environment.NewLine);
                            }
                        }

                        XRPictureBox xRPictureBox1 = new XRPictureBox();
                        if (isNotNullorEmpty(DataProcessing.reportInformation))
                        {


                            if (settings.addOrganizationNameToReport && settings.organizationName.Trim() != string.Empty)
                            {
                                if (obfuscationLevel == 0 || !settings.removeCompanyNameInObfuscatedReports)
                                {
                                    tableRow = new XRTableRow();
                                    tableCell = new XRTableCell();
                                    tableCell.Text = settings.organizationName;
                                    tableCell.TextAlignment = TextAlignment.MiddleCenter;
                                    tableCell.Font = new Font("Calibri", 16);
                                    tableRow.Cells.Add(tableCell);
                                    coverPageTable.Rows.Add(tableRow);
                                }
                            }

                            if (settings.addEnvirontNameToReport && settings.environmentName.Trim() != string.Empty)
                            {
                                if (obfuscationLevel == 0 || !settings.removeEnvironmentNameInObfuscatedReports)
                                {
                                    tableRow = new XRTableRow();
                                    tableCell = new XRTableCell();
                                    tableCell.Text = settings.environmentName;
                                    tableCell.TextAlignment = TextAlignment.MiddleCenter;
                                    tableCell.Font = new Font("Calibri", 16);
                                    tableRow.Cells.Add(tableCell);
                                    coverPageTable.Rows.Add(tableRow);
                                }
                            }


                            xRPictureBox1.ImageSource = new ImageSource((new SvgBitmap(DevExpress.Images.ImageResourceCache.Default.GetSvgImageById("LongDate"))).Render(null, 0.6));
                            xRPictureBox1.Sizing = ImageSizeMode.Squeeze;
                            xRPictureBox1.ImageAlignment = ImageAlignment.MiddleCenter;
                            xRPictureBox1.BeforePrint += XRControl_BeforePrint;
                            xRPictureBox1.HeightF = 75;
                            tableRow = new XRTableRow();
                            tableCell = new XRTableCell();
                            tableCell.Controls.Add(xRPictureBox1);
                            xRPictureBox1.Padding = new PaddingInfo(0, 0, 40, 0);
                            tableRow.Cells.Add(tableCell);
                            coverPageTable.Rows.Add(tableRow);
                            XRLabel dateLabel = new XRLabel();
                            dateLabel.BeforePrint += XRControl_BeforePrint;
                            dateLabel.TextAlignment = TextAlignment.MiddleCenter;
                            DateTime reportCreationDate = new DateTime();
                            DateTime.TryParse(DataProcessing.reportInformation.Rows[0][0].ToString(), out reportCreationDate);
                            dateLabel.Text = reportCreationDate.DayOfWeek + ", " + reportCreationDate.ToString("MMMM") + " " + reportCreationDate.Day + ", " + reportCreationDate.Year + " - " + reportCreationDate.ToString("hh:mm tt");
                            dateLabel.Font = new Font("Calibri", 14);
                            tableCell = new XRTableCell();
                            tableRow = new XRTableRow();
                            tableRow.Cells.Add(tableCell);
                            coverPageTable.Rows.Add(tableRow);
                            tableCell.Controls.Add(dateLabel);

                            environmentReport.Name = reportCreationDate.ToString("yyyy-MM-dd hh:mm tt") + "_" + environmentReport.Name;

                        }

                        XRLabel pasReporterVersionLabel = new XRLabel();
                        pasReporterVersionLabel.Text = "Created with " + MainWindow.PASReporterVersionLong;
                        pasReporterVersionLabel.Font = new Font("Calibri", 12);
                        pasReporterVersionLabel.BeforePrint += XRControl_BeforePrint;
                        pasReporterVersionLabel.TextAlignment = TextAlignment.MiddleCenter;
                        tableCell = new XRTableCell();
                        tableRow = new XRTableRow();
                        tableRow.Padding = new PaddingInfo(0, 0, 120, 0);
                        tableRow.Cells.Add(tableCell);
                        coverPageTable.Rows.Add(tableRow);
                        tableCell.Controls.Add(pasReporterVersionLabel);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("An error occurred while trying to create report cover" + Environment.NewLine + "Error: " + ex.Message);
                        if (ex.InnerException != null)
                        {
                            Console.WriteLine(ex.InnerException.Message);
                        }
                    }
                }

                SubBand tocBand = new SubBand();
                XRTableOfContents xRTableOfContents = new XRTableOfContents();
                xRTableOfContents.Name = "toc";
                xRTableOfContents.LevelTitle.Text = "Table of Contents";
                xRTableOfContents.LevelTitle.Font = new Font("Calibri", 15, FontStyle.Bold);
                xRTableOfContents.LevelTitle.ForeColor = ColorTranslator.FromHtml("#2F5496");
                xRTableOfContents.LevelTitle.Padding = new PaddingInfo(0, 0, 0, 4);
                tocBand.Controls.Add(xRTableOfContents);
                reportHeaderBand.SubBands.Add(tocBand);
                xRTableOfContents.BeforePrint += XRControl_BeforePrint;

                XRTableOfContentsLevel xRTableOfContentsLevel = new XRTableOfContentsLevel();
                xRTableOfContentsLevel.Font = new Font("Calibri", 11, FontStyle.Bold);
                xRTableOfContents.Levels.Add(xRTableOfContentsLevel);

                XRTable pageInfoTable = new XRTable();
                XRTableRow pageInfoRow = new XRTableRow();
                pageInfoTable.Rows.Add(pageInfoRow);
                pageInfoTable.WidthF = getReportWidthWithoutMargins();
                bottomMarginBand.Controls.Add(pageInfoTable);
                XRTableCell footerCellLeft = new XRTableCell();
                XRTableCell footerCellRight = new XRTableCell();
                XRPageInfo xRPageInfo = new XRPageInfo();
                xRPageInfo.Name = "PageNumberInfo";
                footerCellRight.Controls.Add(xRPageInfo);
                footerCellRight.TextAlignment = TextAlignment.BottomRight;
                pageInfoRow.Cells.Add(footerCellLeft);
                pageInfoRow.Cells.Add(footerCellRight);
                xRPageInfo.PrintOnPage += xRPageInfo_PrintOnPage;

                XRTable pageHeaderTable = new XRTable();
                XRTableRow pageHeaderTableRow = new XRTableRow();
                pageHeaderTable.BeforePrint += PageHeaderTable_BeforePrint;
                pageHeaderTable.HeightF = 25;
                pageHeaderTable.Padding = new PaddingInfo(0, 95, 0, 0);
                pageHeaderTable.Rows.Add(pageHeaderTableRow);
                pageHeaderTable.PrintOnPage += xRPageInfo_PrintOnPage;
                XRTableCell pageHeaderCellLeft = new XRTableCell();
                pageHeaderCellLeft.Text = "PAS Environment Report";
                if (obfuscationLevel == 1)
                {
                    pageHeaderCellLeft.Text += " (obfuscated)";
                }
                else if (obfuscationLevel == 2)
                {
                    pageHeaderCellLeft.Text += " (highly obfuscated)";
                }

                pageHeaderCellLeft.WordWrap = false;
                pageHeaderCellLeft.TextAlignment = TextAlignment.BottomRight;
                pageHeaderTableRow.Cells.Add(pageHeaderCellLeft);
                topMarginBand.Controls.Add(pageHeaderTable);


                if (isNotNullorEmpty(DataProcessing.installedVersionsSummary))
                {
                    try
                    {
                        headerLabel = createHeaderLabel("Installed Product Versions");
                        detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40));
                        xRTable = CreateXRTable(DataProcessing.installedVersionsSummary, 0);
                        xRPanel = createXRPanelAndAddControl(headerLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Failed to create installed product versions table" + ex.Message);
                    }
                }

                if (isNotNullorEmpty(DataProcessing.licenceCapacity))
                {
                    try
                    {
                        headerLabel = createHeaderLabel("License Capacity");
                        detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40));
                        xRTable = CreateXRTable(DataProcessing.licenceCapacity, 0, 4);
                        xRTable = resizeColumnsInTable(xRTable, 1, false, 200);
                        xRTable = resizeColumnsInTable(xRTable, 2, false, 50);
                        xRPanel = createXRPanelAndAddControl(headerLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Failed to create license capacity table" + ex.Message);
                    }
                }

                if (isNotNullorEmpty(DataProcessing.accountStatisticsDataTable))
                {
                    headerLabel = createHeaderLabel("Account Management");
                    detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40));

                    subHeaderLabel = createSubHeaderLabel("Account statistics", headerLabel);
                    xRTable = CreateXRTable(DataProcessing.accountStatisticsDataTable, 0, 2);
                    xRTable = resizeColumnsInTable(xRTable, 1, true);
                    xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                    detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                    if (isNotNullorEmpty(DataProcessing.masterPolicySummaryTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Master policy settings", headerLabel);
                        xRTable = CreateXRTable(DataProcessing.masterPolicySummaryTable, 0);
                        //xRTable = resizeColumnsInTable(xRTable, 1, true);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel,-1,true));
                    }

                    if (isNotNullorEmpty(DataProcessing.managedAccountsDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Managed accounts", headerLabel);
                        xRChart = createChart("Managed accounts (%)", DataProcessing.managedAccountsDataTable);
                        addSingleSeriesToChart(xRChart, "Managed accounts (%)", ViewType.Pie, "Managed", "Value", true);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        xRChart.Series[0].ColorDataMember = "Managed";
                        Palette customPalette = new Palette("custom");
                        customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#4DBD33") });
                        customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#ee3647") });
                        customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#fcc340") });
                        KeyColorColorizer colorizer = new KeyColorColorizer();
                        colorizer.Palette = customPalette;
                        colorizer.Keys.Add("Managed Accounts");
                        colorizer.Keys.Add("Unmanaged Accounts");

                        if (DataProcessing.managedAccountsDataTable.AsEnumerable().Where(c => c.Field<string>("Managed").Contains("missing")).Count() > 0)
                        {
                            colorizer.Keys.Add("n/a - missing platform information");
                        }

                        (xRChart.Series[0]).View.Colorizer = colorizer;

                    }

                    if (isNotNullorEmpty(DataProcessing.notManagedReasonsForAccounts))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts not managed reasons", headerLabel);
                        xRChart = createChart("Accounts not managed reasons (several reasons can apply)", DataProcessing.notManagedReasonsForAccounts, false);
                        addSingleSeriesToChart(xRChart, "", ViewType.Bar, "Not managed reason", "Number of accounts", true, true, "{V} accounts");
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }

                    if (checkIfHasManagedAccounts())
                    {
                        if (isNotNullorEmpty(DataProcessing.compliantAccountsDataTable))
                        {

                            subHeaderLabel = createSubHeaderLabel("Compliant accounts", headerLabel);

                            xRChart = createChart("Compliant accounts (%)", DataProcessing.compliantAccountsDataTable);
                            addSingleSeriesToChart(xRChart, "Compliant accounts (%)", ViewType.Pie, "Compliant", "Accounts", true);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            Palette customPalette = new Palette("custom");
                            customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#4DBD33") });
                            customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#ee3647") });
                            KeyColorColorizer colorizer = new KeyColorColorizer();
                            colorizer.Palette = customPalette;
                            colorizer.Keys.Add("Compliant Accounts");
                            colorizer.Keys.Add("Non-Compliant Accounts");

                            xRChart.Series[0].ColorDataMember = "Compliant";
                            customPalette = new Palette("custom");
                            customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#4DBD33") });
                            customPalette.Add(new PaletteEntry() { Color = ColorTranslator.FromHtml("#ee3647") });
                            colorizer = new KeyColorColorizer();
                            colorizer.Palette = customPalette;
                            colorizer.Keys.Add("Compliant Accounts");
                            colorizer.Keys.Add("Non-Compliant Accounts");
                            (xRChart.Series[0]).View.Colorizer = colorizer;

                            xRChart = createChart("Compliant accounts", DataProcessing.compliantAccountsDataTable);
                            addSingleSeriesToChart(xRChart, "Compliant accounts", ViewType.Bar, "Compliant", "Accounts", true, true);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            xRChart.Series[0].ColorDataMember = "Compliant";
                            (xRChart.Series[0]).View.Colorizer = colorizer;
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.accountsPerDeviceTypeDataTable))
                    {

                        subHeaderLabel = createSubHeaderLabel("Accounts by device type", headerLabel);


                        if (obfuscationLevel == 2)
                        {
                            xRChart = createChart("Accounts by device type (%) - Top 10", obfuscateDataTable(DataProcessing.accountsPerDeviceTypeDataTable, false, "Device type"));
                        }
                        else
                        {
                            xRChart = createChart("Accounts by device type (%) - Top 10", DataProcessing.accountsPerDeviceTypeDataTable);
                        }
                        addSingleSeriesToChart(xRChart, "Accounts by device type (%) - Top 10", ViewType.Pie, "DeviceType", "NumberOfAccounts", true, false, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        if (obfuscationLevel == 2)
                        {
                            xRChart = createChart("Accounts by device type - Top 10", obfuscateDataTable(DataProcessing.accountsPerDeviceTypeDataTable, false, "Device type"));
                        }
                        else
                        {
                            xRChart = createChart("Accounts by device type - Top 10", DataProcessing.accountsPerDeviceTypeDataTable);
                        }
                        addSingleSeriesToChart(xRChart, "Accounts by device type - Top 10", ViewType.Bar, "DeviceType", "NumberOfAccounts", true, true, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(null, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }


                    if (isNotNullorEmpty(DataProcessing.accountsPerPolicyDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts by policy", headerLabel);


                        if (obfuscationLevel == 2)
                        {
                            xRChart = createChart("Accounts by policy (%) - Top 10", obfuscateDataTable(DataProcessing.accountsPerPolicyDataTable, false, "Policy"));
                        }
                        else
                        {
                            xRChart = createChart("Accounts by policy (%) - Top 10", DataProcessing.accountsPerPolicyDataTable);
                        }
                        addSingleSeriesToChart(xRChart, "Accounts by policy (%) - Top 10", ViewType.Pie, "PolicyID", "NumberOfAccounts", true, false, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        if (obfuscationLevel == 2)
                        {
                            xRChart = createChart("Accounts by policy - Top 10", obfuscateDataTable(DataProcessing.accountsPerPolicyDataTable, false, "Policy"));
                        }
                        else
                        {
                            xRChart = createChart("Accounts by policy - Top 10", DataProcessing.accountsPerPolicyDataTable);
                        }
                        addSingleSeriesToChart(xRChart, "Accounts by policy - Top 10", ViewType.Bar, "PolicyID", "NumberOfAccounts", true, true, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(null, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }


                    if (isNotNullorEmpty(DataProcessing.accountsByCreationMethod))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts by creation method", headerLabel);
                        xRChart = createChart("Accounts by creation method (%)", DataProcessing.accountsByCreationMethod);
                        addSingleSeriesToChart(xRChart, "Accounts by creation method (%)", ViewType.Pie, "CreationMethod", "Accounts", true);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }

                    if (isNotNullorEmpty(DataProcessing.accountsByCreationMethod))
                    {
                        xRChart = createChart("Accounts by creation method", DataProcessing.accountsByCreationMethod);
                        addSingleSeriesToChart(xRChart, "Accounts by creation method", ViewType.Bar, "CreationMethod", "Accounts", true, true);
                        xRPanel = createXRPanelAndAddControl(null, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }


                    if (isNotNullorEmpty(DataProcessing.accountCreatorsDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts creators", headerLabel);
                        chartTitle = "Account creators (%) - Top 10 ";

                        if (obfuscationLevel > 0)
                        {
                            xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountCreatorsDataTable, false, "User"));
                        }
                        else
                        {
                            xRChart = createChart(chartTitle, DataProcessing.accountCreatorsDataTable);
                        }

                        addSingleSeriesToChart(xRChart, "Accounts creators (%) - Top 10 ", ViewType.Pie, "UserName", "CreatedAccounts", true, false, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }

                    if (isNotNullorEmpty(DataProcessing.accountCreatorsDataTable))
                    {
                        chartTitle = "Account creators - Top 10";

                        if (obfuscationLevel > 0)
                        {
                            xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountCreatorsDataTable, false, "User"));
                        }
                        else
                        {
                            xRChart = createChart(chartTitle, DataProcessing.accountCreatorsDataTable);
                        }

                        addSingleSeriesToChart(xRChart, "Accounts creators - Top 10", ViewType.Bar, "UserName", "CreatedAccounts", true, true, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(null, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }

                    if (isNotNullorEmpty(DataProcessing.accountDeletersDataTable))
                    {
                        chartTitle = "Account removers (%) - Top 10 ";
                        subHeaderLabel = createSubHeaderLabel("Account removers", headerLabel);

                        if (obfuscationLevel > 0)
                        {
                            xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountDeletersDataTable, false, "User"));
                        }
                        else
                        {
                            xRChart = createChart(chartTitle, DataProcessing.accountDeletersDataTable);
                        }

                        addSingleSeriesToChart(xRChart, "Accounts removers (%) - Top 10 ", ViewType.Pie, "UserName", "RemovedAccounts", true, false, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }

                    if (isNotNullorEmpty(DataProcessing.accountDeletersDataTable))
                    {
                        chartTitle = "Account removers - Top 10";

                        if (obfuscationLevel > 0)
                        {
                            xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountDeletersDataTable, false, "User"));
                        }
                        else
                        {
                            xRChart = createChart(chartTitle, DataProcessing.accountDeletersDataTable);
                        }

                        addSingleSeriesToChart(xRChart, "Accounts removers - Top 10", ViewType.Bar, "UserName", "RemovedAccounts", true, true, null, null, 10);
                        xRPanel = createXRPanelAndAddControl(null, xRChart);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                    }


                    if (isNotNullorEmpty(DataProcessing.accountsDisabledBy))
                    {
                        try
                        {
                            subHeaderLabel = createSubHeaderLabel("Accounts disabled by", headerLabel);

                            xRChart = createChart("Accounts disabled by (%)", DataProcessing.accountsDisabledBy);
                            addSingleSeriesToChart(xRChart, "Accounts disabled by (%)", ViewType.Pie, "DisabledBy", "NumberOfAccounts", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            xRChart.PaletteName = "Solstice"; //Marquee //Solstice

                            xRChart = createChart("Accounts disabled by", DataProcessing.accountsDisabledBy);
                            addSingleSeriesToChart(xRChart, "Accounts disabled by", ViewType.Bar, "DisabledBy", "NumberOfAccounts", true, true);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.cpmInformationDataTable) && DataProcessing.cpmInformationDataTable.Rows.Count > 1)
                    {
                        var dataTable = new DataTable();
                        try
                        {
                            subHeaderLabel = createSubHeaderLabel("Accounts per CPM", headerLabel);

                            chartTitle = "Accounts per CPM";

                            dataTable = (from row in DataProcessing.cpmInformationDataTable.AsEnumerable() where row.Field<string>("CPM") != "No CPM" select row).CopyToDataTable();

                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.cpmInformationDataTable, false, "CPM"));
                            }
                            else
                            {
                                xRChart = createChart(chartTitle, DataProcessing.cpmInformationDataTable);
                            }

                            addSingleSeriesToChart(xRChart, "Accounts per CPM", ViewType.Bar, "CPM", "Accounts", true);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }

                        if (checkIfHasManagedAccounts())
                        {
                            try
                            {
                                chartTitle = "Managed accounts per CPM";

                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart(chartTitle, obfuscateDataTable(dataTable, false, "CPM"));
                                }
                                else
                                {
                                    xRChart = createChart(chartTitle, dataTable);
                                }

                                addSingleSeriesToChart(xRChart, "Managed accounts per CPM", ViewType.Bar, "CPM", "ManagedAccounts", true);
                                xRChart.Series[0].FilterString = "CPM  != No CPM";
                                xRPanel = createXRPanelAndAddControl(null, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.accountsDevelopmentDataTable))
                    {
                        try
                        {
                            subHeaderLabel = createSubHeaderLabel("Accounts development", headerLabel);
                            xRChart = createChart("Accounts development", DataProcessing.accountsDevelopmentDataTable);
                            addSingleSeriesToChart(xRChart, "Total number of accounts", ViewType.Line, "Date", "NumberOfAccounts");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.accountDevelopmentByPolicy))
                    {
                        chartTitle = "Accounts development by policy - Top 5";

                        try
                        {
                            if (obfuscationLevel == 2)
                            {
                                xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountDevelopmentByPolicy, true, "Policy"));
                            }
                            else
                            {
                                xRChart = createChart(chartTitle, DataProcessing.accountDevelopmentByPolicy);
                            }

                            subHeaderLabel = createSubHeaderLabel("Accounts development by policy", headerLabel);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 1, 5);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.accountDevelopmentByDeviceType))
                    {
                        try
                        {
                            chartTitle = "Accounts development by device type - Top 5";

                            if (obfuscationLevel == 2)
                            {
                                xRChart = createChart(chartTitle, obfuscateDataTable(DataProcessing.accountDevelopmentByDeviceType, true, "Device type"));
                            }
                            else
                            {
                                xRChart = createChart(chartTitle, DataProcessing.accountDevelopmentByDeviceType);
                            }
                            subHeaderLabel = createSubHeaderLabel("Accounts development by device type", headerLabel);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 1, 5);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.accountDevelopmentByCreationMethod))
                    {
                        try
                        {
                            chartTitle = "Accounts development by creation method";
                            subHeaderLabel = createSubHeaderLabel(chartTitle, headerLabel);
                            xRChart = createChart(chartTitle, DataProcessing.accountDevelopmentByCreationMethod);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 1);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }
                }


                if (isNotNullorEmpty(DataProcessing.safesStatisticsDataTable))
                {
                    headerLabel = createHeaderLabel("Safe Management");
                    detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40));

                    subHeaderLabel = createSubHeaderLabel("Safe statistics", headerLabel);
                    xRTable = CreateXRTable(DataProcessing.safesStatisticsDataTable, 0, 2);
                    xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                    detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));

                    if (isNotNullorEmpty(DataProcessing.objectsInVault))
                    {
                        try
                        {
                            subHeaderLabel = createSubHeaderLabel("Objects in Vault", headerLabel);

                            xRChart = createChart("Objects in Vault (%)", DataProcessing.objectsInVault);
                            addSingleSeriesToChart(xRChart, "Objects in Vault (%)", ViewType.Pie, "Object Type", "NumberOfObjects", true);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            xRChart = createChart("Objects in Vault", DataProcessing.objectsInVault);
                            addSingleSeriesToChart(xRChart, "Objects in Vault", ViewType.Bar, "Object Type", "NumberOfObjects", true, true);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.safesObjectFilesAccountsOrderedByAccounts))
                    {
                        try
                        {
                            DataTable accountsPerSafeExludingDeleted = DataProcessing.safesObjectFilesAccountsOrderedByAccounts.Copy();
                            accountsPerSafeExludingDeleted.DefaultView.Sort = "Accounts (excluding deleted accounts) DESC";
                            accountsPerSafeExludingDeleted = accountsPerSafeExludingDeleted.DefaultView.ToTable().AsEnumerable().Take(10).CopyToDataTable();

                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Accounts per safe (excluding deleted accounts) - Top 10", obfuscateDataTable(accountsPerSafeExludingDeleted, false, "Safe"));
                            }
                            else
                            {
                                xRChart = createChart("Accounts per safe (excluding deleted accounts) - Top 10", accountsPerSafeExludingDeleted);
                            }


                            subHeaderLabel = createSubHeaderLabel("Objects per safe", headerLabel);
                            addSingleSeriesToChart(xRChart, "Accounts per safe", ViewType.Bar, "SafeName", "Accounts (excluding deleted accounts)", true, true);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }

                        if (isNotNullorEmpty(DataProcessing.safesObjectFilesAccountsOrderedByFiles))
                        {
                            try
                            {
                                DataTable filesPerSafeExludingDeleted = DataProcessing.safesObjectFilesAccountsOrderedByFiles.Copy();
                                filesPerSafeExludingDeleted.DefaultView.Sort = "Files (excluding deleted files) DESC";
                                filesPerSafeExludingDeleted = filesPerSafeExludingDeleted.DefaultView.ToTable().AsEnumerable().Take(10).CopyToDataTable();

                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart("Files per safe (excluding deleted files) - Top 10", obfuscateDataTable(filesPerSafeExludingDeleted, false, "Safe"));
                                }
                                else
                                {
                                    xRChart = createChart("Files per safe (excluding deleted files) - Top 10", filesPerSafeExludingDeleted);
                                }

                                addSingleSeriesToChart(xRChart, "Files per safe", ViewType.Bar, "SafeName", "Files (excluding deleted files)", true, true);
                                xRPanel = createXRPanelAndAddControl(null, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }


                        if (isNotNullorEmpty(DataProcessing.largestSafes))
                        {

                            try
                            {
                                subHeaderLabel = createSubHeaderLabel("Safes by used size", headerLabel);

                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart("Safes by used size (GB) - Top 10", obfuscateDataTable(DataProcessing.largestSafes, false, "Safe"));
                                }
                                else
                                {
                                    xRChart = createChart("Safes by used size (GB) - Top 10", DataProcessing.largestSafes);
                                }

                                addSingleSeriesToChart(xRChart, "Safes by used size (GB)", ViewType.Bar, "SafeName", "Size", true, true, "{V} GB", null, 10);
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                                var largestSafesPercent = DataProcessing.largestSafes.Copy();
                                largestSafesPercent.DefaultView.Sort = "UsedSize DESC";
                                largestSafesPercent = largestSafesPercent.DefaultView.ToTable().AsEnumerable().Take(10).CopyToDataTable();

                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart("Safes by used size (%) - Top 10", obfuscateDataTable(largestSafesPercent, false, "Safe"));
                                }
                                else
                                {
                                    xRChart = createChart("Safes by used size (%) - Top 10", largestSafesPercent);
                                }

                                addSingleSeriesToChart(xRChart, "Safes by used size (%)", ViewType.Bar, "SafeName", "UsedSize", true, true, "{V}%", null, 10);
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }

                        if (isNotNullorEmpty(DataProcessing.topSafeCreators))
                        {
                            try
                            {
                                subHeaderLabel = createSubHeaderLabel("Safe creators", headerLabel);

                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart("Safes creators (%) - Top 5", obfuscateDataTable(DataProcessing.topSafeCreators, false, "User"));
                                }
                                else
                                {
                                    xRChart = createChart("Safes creators (%) - Top 5", DataProcessing.topSafeCreators);
                                }

                                addSingleSeriesToChart(xRChart, "Safes creators (%) - Top 5", ViewType.Pie, "UserName", "CreatedSafes", true, false, null, null, 5);
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                                if (obfuscationLevel > 0)
                                {
                                    xRChart = createChart("Safes creators - Top 5", obfuscateDataTable(DataProcessing.topSafeCreators, false, "User"));
                                }
                                else
                                {
                                    xRChart = createChart("Safes creators - Top 5", DataProcessing.topSafeCreators);
                                }

                                addSingleSeriesToChart(xRChart, "Safes creators - Top 5", ViewType.Bar, "UserName", "CreatedSafes", true, true, null, null, 5);
                                xRPanel = createXRPanelAndAddControl(null, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }

                        if (isNotNullorEmpty(DataProcessing.safesByRetentionType))
                        {
                            subHeaderLabel = createSubHeaderLabel("Safes by retention type", headerLabel);
                            try
                            {
                                xRChart = createChart("Safes by retention type (%)", DataProcessing.safesByRetentionType);
                                addSingleSeriesToChart(xRChart, "Safes by retention type (%)", ViewType.Pie, "RetentionType", "Safes", true, false, null, null);
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                                xRChart.PaletteName = "Solstice";

                                xRChart = createChart("Safes by retention type", DataProcessing.safesByRetentionType);
                                addSingleSeriesToChart(xRChart, "Safes by retention type)", ViewType.Bar, "RetentionType", "Safes", true, true, "{V} safes");
                                xRPanel = createXRPanelAndAddControl(null, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }


                        if (isNotNullorEmpty(DataProcessing.mostUsedSafeRetentionSettings))
                        {
                            subHeaderLabel = createSubHeaderLabel("Most used retention settings", headerLabel);
                            try
                            {
                                xRChart = createChart("Most used retention settings (%) - Top 5", DataProcessing.mostUsedSafeRetentionSettings);
                                addSingleSeriesToChart(xRChart, "Most used retention settings (%) - Top 5", ViewType.Pie, "RetentionSetting", "Safes", true, false, null, null, 5);
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                                xRChart = createChart("Most used retention settings - Top 5", DataProcessing.mostUsedSafeRetentionSettings);
                                addSingleSeriesToChart(xRChart, "Most used retention settings - Top 5", ViewType.Bar, "RetentionSetting", "Safes", true, true, "{V} safes", null, 5);
                                xRPanel = createXRPanelAndAddControl(null, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }


                        if (isNotNullorEmpty(DataProcessing.safesCreationDataTable))
                        {
                            subHeaderLabel = createSubHeaderLabel("Safes creation", headerLabel);
                            try
                            {
                                xRChart = createChart("Safes creation", DataProcessing.safesCreationDataTable);
                                addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }


                        if (isNotNullorEmpty(DataProcessing.objectsCountByPeriod))
                        {
                            subHeaderLabel = createSubHeaderLabel("Objects development", headerLabel);
                            try
                            {
                                xRChart = createChart("Objects development", DataProcessing.objectsCountByPeriod);
                                addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }

                        if (isNotNullorEmpty(DataProcessing.totalFilesGrowth))
                        {
                            subHeaderLabel = createSubHeaderLabel("Data development ", headerLabel);
                            try
                            {
                                xRChart = createChart("Total size of all objects (GB)", DataProcessing.totalFilesGrowth);
                                addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                                xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                                detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                                Console.WriteLine(ex.Message);
                            }
                        }
                    }
                }

                if (isNotNullorEmpty(DataProcessing.sessionStatisticsDataTable))
                {
                    headerLabel = createHeaderLabel("Session Management");
                    detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40, false, true));
                    subHeaderLabel = createSubHeaderLabel("Session statistics ", headerLabel);

                    try
                    {
                        xRTable = CreateXRTable(DataProcessing.sessionStatisticsDataTable, 0);
                        xRTable = resizeColumnsInTable(xRTable, 0, false, 130);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                        Console.WriteLine(ex.Message);
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByPsmSolution) && DataProcessing.sessionsByPsmSolution.Rows.Count > 1)
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by PSM solution", headerLabel);

                        try
                        {

                            xRChart = createChart("Sessions by PSM solution (%)", DataProcessing.sessionsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Sessions by PSM solution (%)", ViewType.Pie, "PSM Solution", "Sessions", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));
                            //xRChart.PaletteName = "Solstice";

                            xRChart = createChart("Sessions by PSM solution", DataProcessing.sessionsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Sessions by PSM solution", ViewType.Bar, "PSM Solution", "Sessions", true, true, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByPsmID))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by PSM solution server ID ", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by PSM solution server ID (%)", obfuscateDataTable(DataProcessing.sessionsByPsmID, false, "Server ID"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by PSM solution server ID (%)", DataProcessing.sessionsByPsmID);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by PSM solution server ID (%)", ViewType.Pie, "SolutionServerID", "Sessions", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));

                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by PSM solution server ID", obfuscateDataTable(DataProcessing.sessionsByPsmID, false, "Server ID"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by PSM solution server ID", DataProcessing.sessionsByPsmID);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by PSM solution server ID", ViewType.Bar, "SolutionServerID", "Sessions", true, true, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsPerWeekday))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by weekday", headerLabel);

                        try
                        {

                            xRChart = createChart("Sessions by weekday (%)", DataProcessing.sessionsPerWeekday);
                            addSingleSeriesToChart(xRChart, "Sessions by weekday (%)", ViewType.Pie, "Weekday", "Sessions", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));

                            xRChart = createChart("Sessions by weekday", DataProcessing.sessionsPerWeekday);
                            addSingleSeriesToChart(xRChart, "Sessions by PSM solution", ViewType.Bar, "weekday", "Sessions", true, false, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.sessionsByHour))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by daytime hour", headerLabel);

                        try
                        {
                            var sortedDataTable = DataProcessing.sessionsByHour.Copy();
                            sortedDataTable.DefaultView.Sort = "Sessions desc";
                            sortedDataTable = sortedDataTable.DefaultView.ToTable();

                            xRChart = createChart("Sessions by daytime hour (%) - Top 5", sortedDataTable);
                            addSingleSeriesToChart(xRChart, "Sessions by daytime hour (%) - Top 5", ViewType.Pie, "Hour", "Sessions", true, false, null, "{A} hh", 5);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));

                            xRChart = createChart("Sessions by daytime hour", DataProcessing.sessionsByHour);
                            addSingleSeriesToChart(xRChart, "Sessions by daytime hour", ViewType.Bar, "Hour", "Sessions", true, false, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByConnectionComponent))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by connection component", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by connection component (%) - Top 10", obfuscateDataTable(DataProcessing.sessionsByConnectionComponent, false, "Connection component"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by connection component (%) - Top 10", DataProcessing.sessionsByConnectionComponent);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by connection component (%) - Top 10", ViewType.Pie, "ConnectionComponent", "Sessions", true, false, null, null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by connection component - Top 10", obfuscateDataTable(DataProcessing.sessionsByConnectionComponent, false, "Connection component"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by connection component - Top 10", DataProcessing.sessionsByConnectionComponent);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by connection component - Top 10", ViewType.Bar, "ConnectionComponent", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByPolicy))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by policy", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by policy (%) - Top 10", obfuscateDataTable(DataProcessing.sessionsByPolicy, false, "Policy"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by policy (%) - Top 10", DataProcessing.sessionsByPolicy);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by policy (%) - Top 10", ViewType.Pie, "PolicyID", "Sessions", true, false, null, null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by policy - Top 10", obfuscateDataTable(DataProcessing.sessionsByPolicy, false, "Policy"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by policy - Top 10", DataProcessing.sessionsByPolicy);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by policy - Top 10", ViewType.Bar, "PolicyID", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByDeviceType))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by device type", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by device type (%) - Top 10", obfuscateDataTable(DataProcessing.sessionsByDeviceType, false, "Device type"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by device type (%) - Top 10", DataProcessing.sessionsByDeviceType);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by device type (%) - Top 10", ViewType.Pie, "DeviceType", "Sessions", true, false, null, null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by device type - Top 10", obfuscateDataTable(DataProcessing.sessionsByDeviceType, false, "Device type"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by device type - Top 10", DataProcessing.sessionsByDeviceType);
                            }

                            addSingleSeriesToChart(xRChart, "Sessions by device type - Top 10", ViewType.Bar, "DeviceType", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByConnectionType) && DataProcessing.sessionsByConnectionType.Rows.Count > 1)
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by connection type", headerLabel);

                        try
                        {
                            xRChart = createChart("Sessions by connection type (%)", DataProcessing.sessionsByConnectionType);
                            addSingleSeriesToChart(xRChart, "Sessions by connection type", ViewType.Pie, "ConnectionType", "Sessions", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));
                            xRChart.PaletteName = "Solstice";

                            xRChart = createChart("Sessions by connection type", DataProcessing.sessionsByConnectionType);
                            addSingleSeriesToChart(xRChart, "Sessions by connection type", ViewType.Bar, "ConnectionType", "Sessions", true, true, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }



                    if (isNotNullorEmpty(DataProcessing.sessionsBySessionDuration) && isNotNullorEmpty(DataProcessing.sessionsBySessionDuration2))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by session duration", headerLabel);

                        try
                        {
                            xRChart = createChart("Sessions by session duration (%)", DataProcessing.sessionsBySessionDuration2);
                            addSingleSeriesToChart(xRChart, "Sessions by session duration (%)", ViewType.Pie, "Duration", "Sessions", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            xRChart = createChart("Sessions by session duration", DataProcessing.sessionsBySessionDuration);
                            addSingleSeriesToChart(xRChart, "Sessions by session duration", ViewType.Bar, "Duration", "Sessions", true, true, "{V} sessions");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.targetSystemsByPsmSolution))
                    {

                        subHeaderLabel = createSubHeaderLabel("Target systems by PSM solution", headerLabel);

                        try
                        {
                            xRChart = createChart("Target systems by PSM solution (%)", DataProcessing.targetSystemsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Target systems by PSM solution (%)", ViewType.Pie, "PSM Solution", "TargetHosts", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            xRChart = createChart("Target systems by PSM solution", DataProcessing.targetSystemsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Target systems by PSM solution", ViewType.Bar, "PSM Solution", "TargetHosts", true, true, "{V} targets");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.targetAccountsByPsmSolution))
                    {

                        subHeaderLabel = createSubHeaderLabel("Target accounts by PSM solution", headerLabel);

                        try
                        {
                            xRChart = createChart("Target accounts by PSM solution (%)", DataProcessing.targetAccountsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Target accounts by PSM solution (%)", ViewType.Pie, "PSM Solution", "TargetAccounts", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, false, true));


                            xRChart = createChart("Target accounts by PSM solution", DataProcessing.targetAccountsByPsmSolution);
                            addSingleSeriesToChart(xRChart, "Target accounts by PSM solution", ViewType.Bar, "PSM Solution", "TargetAccounts", true, true, "{V} target accounts");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByCyberArkUser))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions per user", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Sessions per user - Top 10", obfuscateDataTable(DataProcessing.sessionsByCyberArkUser, false, "User"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions per user - Top 10", DataProcessing.sessionsByCyberArkUser);
                            }
                            addSingleSeriesToChart(xRChart, "Sessions by connection type", ViewType.Bar, "User", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByTargetTop10))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions per target system", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Sessions per target system - Top 10", obfuscateDataTable(DataProcessing.sessionsByTargetTop10, false, "Target system"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions per target system - Top 10", DataProcessing.sessionsByTargetTop10);
                            }
                            addSingleSeriesToChart(xRChart, "Sessions per target system", ViewType.Bar, "TargetHost", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.sessionsByTargetAccountTop10))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions per target account", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Sessions per target account - Top 10", obfuscateDataTable(DataProcessing.sessionsByTargetAccountTop10, false, "Target account"));
                            }
                            else
                            {
                                xRChart = createChart("Sessions per target account - Top 10", DataProcessing.sessionsByTargetAccountTop10);
                            }
                            addSingleSeriesToChart(xRChart, "Sessions per target account", ViewType.Bar, "TargetAccount", "Sessions", true, true, "{V} sessions", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByPsmID))
                    {

                        subHeaderLabel = createSubHeaderLabel("Max concurrent sessions by PSM server ID", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Max concurrent sessions by PSM server ID", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByPsmID, false, "PSM ID"));
                            }
                            else
                            {
                                xRChart = createChart("Max concurrent sessions by PSM server ID", DataProcessing.maxConcurrentSessionsByPsmID);
                            }
                            addSingleSeriesToChart(xRChart, "Max concurrent sessions by PSM server ID", ViewType.Bar, "PSM Server ID", "ConcurrentSessions", true, true, "{V}");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByConnectionComponent))
                    {

                        subHeaderLabel = createSubHeaderLabel("Max concurrent sessions by connection component", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Max concurrent sessions by connection component - Top 10", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByConnectionComponent, false, "Connection component"));
                            }
                            else
                            {
                                xRChart = createChart("Max concurrent sessions by connection component - Top 10", DataProcessing.maxConcurrentSessionsByConnectionComponent);
                            }
                            addSingleSeriesToChart(xRChart, "Max concurrent sessions by connection component - Top 10", ViewType.Bar, "ConnectionComponent", "ConcurrentSessions", true, true, "{V}", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByUser))
                    {

                        subHeaderLabel = createSubHeaderLabel("Max concurrent sessions by user", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Max concurrent sessions by user - Top 10", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByUser, false, "User"));
                            }
                            else
                            {
                                xRChart = createChart("Max concurrent sessions by user - Top 10", DataProcessing.maxConcurrentSessionsByUser);
                            }
                            addSingleSeriesToChart(xRChart, "Max concurrent sessions by user - Top 10", ViewType.Bar, "Username", "ConcurrentSessions", true, true, "{V}", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.initiatedSessionsPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions per day", headerLabel);

                        try
                        {

                            xRChart = createChart("Sessions per day", DataProcessing.initiatedSessionsPerDay);

                            addSingleSeriesToChart(xRChart, "Sessions per day", ViewType.Line, "Date", "Sessions");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.initiatedSessionsByPsmSolutionPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by PSM solution per day", headerLabel);

                        try
                        {
                            xRChart = createChart("Sessions by PSM solution per day", DataProcessing.initiatedSessionsByPsmSolutionPerDay);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.initiatedSessionsByPsmIdPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by PSM server ID per day", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by PSM server ID per day - Top 5", obfuscateDataTable(DataProcessing.initiatedSessionsByPsmIdPerDay, true, "PSM ID", 0, false, 2));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by PSM server ID per day - Top 5", DataProcessing.initiatedSessionsByPsmIdPerDay);
                            }
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2, 6);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.initiatedSessionsByConnectionComponentPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Sessions by connection component per day", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Sessions by connection component per day - Top 5", obfuscateDataTable(DataProcessing.initiatedSessionsByConnectionComponentPerDay, true, "Connection component", 0, false, 2));
                            }
                            else
                            {
                                xRChart = createChart("Sessions by connection component per day - Top 5", DataProcessing.initiatedSessionsByConnectionComponentPerDay);
                            }
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2, 6);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessions))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent sessions per day", headerLabel);

                        try
                        {

                            xRChart = createChart("Maximum concurrent sessions per day", DataProcessing.maxConcurrentSessions);
                            addSingleSeriesToChart(xRChart, "Maximum concurrent sessions per day", ViewType.Line, "Date", "MaxConcurrentSessions");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByPsmSolutionPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent sessions by PSM solution per day", headerLabel);

                        try
                        {
                            xRChart = createChart("Maximum concurrent sessions by PSM solution per day", DataProcessing.maxConcurrentSessionsByPsmSolutionPerDay);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.concurrentOpmSessions))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent OPM sessions per day", headerLabel);

                        try
                        {
                            xRChart = createChart("Maximum concurrent OPM sessions per day", DataProcessing.concurrentOpmSessions);
                            addSingleSeriesToChart(xRChart, "Maximum concurrent OPM sessions per day", ViewType.Line, "Date", "MaxConcurrentSessions");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByPsmIDPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent sessions by PSM server ID per day", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Maximum concurrent sessions by PSM server ID per day - Top 5", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByPsmIDPerDay, true, "PSM ID", 0, false, 2));
                            }
                            else
                            {
                                xRChart = createChart("Maximum concurrent sessions by PSM server ID per day - Top 5", DataProcessing.maxConcurrentSessionsByPsmIDPerDay);
                            }

                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2, 6);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByConnectionComponentPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent sessions by connection component per day", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Max. concurrent sessions by connection comp. per day - Top 5", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByConnectionComponentPerDay, true, "Connection component", 0, false, 2));
                            }
                            else
                            {
                                xRChart = createChart("Max. concurrent sessions by connection comp. per day - Top 5", DataProcessing.maxConcurrentSessionsByConnectionComponentPerDay);
                            }

                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2, 6);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.maxConcurrentSessionsByUserPerDay))
                    {

                        subHeaderLabel = createSubHeaderLabel("Maximum concurrent sessions by connection component per day", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Maximum concurrent sessions by user per day - Top 5", obfuscateDataTable(DataProcessing.maxConcurrentSessionsByUserPerDay, true, "User", 0, false, 2));
                            }
                            else
                            {
                                xRChart = createChart("Maximum concurrent sessions by user per day - Top 5", DataProcessing.maxConcurrentSessionsByUserPerDay);
                            }

                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 2, 6);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.recordingsDevelopment))
                    {

                        subHeaderLabel = createSubHeaderLabel("Recordings development", headerLabel);

                        try
                        {

                            xRChart = createChart("Recordings development", DataProcessing.recordingsDevelopment);
                            addSingleSeriesToChart(xRChart, "Recordings development", ViewType.StackedBar, "Date", "Recordings");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.recordingsSizeDevelopment))
                    {

                        subHeaderLabel = createSubHeaderLabel("Recordings size development", headerLabel);

                        try
                        {

                            xRChart = createChart("Recordings size development (GB)", DataProcessing.recordingsSizeDevelopment);
                            addSingleSeriesToChart(xRChart, "Recordings size development (GB)", ViewType.Area, "Date", "Size (GB)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }
                }


                if (isNotNullorEmpty(DataProcessing.userStatisticsDataTable))
                {

                    headerLabel = createHeaderLabel("User management");
                    detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40, false, true));
                    subHeaderLabel = createSubHeaderLabel("User statistics", headerLabel);

                    try
                    {
                        xRTable = CreateXRTable(DataProcessing.userStatisticsDataTable, 0);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                        Console.WriteLine(ex.Message);
                    }

                    if (isNotNullorEmpty(DataProcessing.usersByType))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by user type", headerLabel);

                        try
                        {
                            xRChart = createChart("Users by user type (%) - Top 3", DataProcessing.usersByType);
                            addSingleSeriesToChart(xRChart, "Users by user type (%)", ViewType.Pie, "UserType", "Users", true, false, null, null, 3);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            xRChart = createChart("Users by user type - Top 5", DataProcessing.usersByType);
                            addSingleSeriesToChart(xRChart, "Users by user type", ViewType.Bar, "UserType", "Users", true, true, "{V} users", null, 5);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersByOrigin))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by origin", headerLabel);

                        try
                        {
                            xRChart = createChart("Users by origin (%)", DataProcessing.usersByOrigin);
                            addSingleSeriesToChart(xRChart, "Users by origin (%)", ViewType.Pie, "Origin", "Users", true, false, null, null, 3);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            xRChart.PaletteName = "Solstice";

                            xRChart = createChart("Users by origin", DataProcessing.usersByOrigin);
                            addSingleSeriesToChart(xRChart, "Users by origin", ViewType.Bar, "Origin", "Users", true, true, "{V} users", null, 5);
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.mappingsDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by mapping", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users by mapping (%)", obfuscateDataTable(DataProcessing.mappingsDataTable, false, "Mapping", 1));
                            }
                            else
                            {
                                xRChart = createChart("Users by mapping (%)", DataProcessing.mappingsDataTable);
                            }

                            addSingleSeriesToChart(xRChart, "Users by mapping (%)", ViewType.Pie, "MapName", "Users", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users by mapping", obfuscateDataTable(DataProcessing.mappingsDataTable, false, "Mapping", 1));
                            }
                            else
                            {
                                xRChart = createChart("Users by mapping", DataProcessing.mappingsDataTable);
                            }

                            addSingleSeriesToChart(xRChart, "Users by mapping", ViewType.Bar, "MapName", "Users", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.usersByLDAP))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by LDAP directory", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users by LDAP directory (%)", obfuscateDataTable(DataProcessing.usersByLDAP, false, "LDAP directory"));
                            }
                            else
                            {
                                xRChart = createChart("Users by LDAP directory (%)", DataProcessing.usersByLDAP);
                            }

                            addSingleSeriesToChart(xRChart, "Users by LDAP directory (%)", ViewType.Pie, "LDAPDirectory", "Users", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users by LDAP directory", obfuscateDataTable(DataProcessing.usersByLDAP, false, "LDAP directory"));
                            }
                            else
                            {
                                xRChart = createChart("Users by LDAP directory", DataProcessing.usersByLDAP);
                            }

                            addSingleSeriesToChart(xRChart, "Users by LDAP directory", ViewType.Bar, "LDAPDirectory", "Users", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersByLastLogon2))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by last logon", headerLabel);

                        try
                        {

                            xRChart = createChart("Users by last logon (%)", DataProcessing.usersByLastLogon2);
                            addSingleSeriesToChart(xRChart, "Users by last logon (%)", ViewType.Pie, "LastLogon", "Users", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            xRChart = createChart("Users by last logon", DataProcessing.usersByLastLogon);
                            addSingleSeriesToChart(xRChart, "Users by last logon", ViewType.Bar, "LastLogon", "Users", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.epvUsersByAuthenticationMethod))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users by authentication method", headerLabel);

                        try
                        {

                            xRChart = createChart("EPV users by authentication method (%)", DataProcessing.epvUsersByAuthenticationMethod);
                            addSingleSeriesToChart(xRChart, "EPV users by authentication method (%)", ViewType.Pie, "AuthenticationMethod", "EPVUsers", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                            xRChart = createChart("EPV users by authentication method", DataProcessing.epvUsersByAuthenticationMethod);
                            addSingleSeriesToChart(xRChart, "EPV users by authentication method", ViewType.Bar, "AuthenticationMethod", "EPVUsers", true, true, "{V} EPV users");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));


                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.userAuthorizationsDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("User authorizations", headerLabel);

                        try
                        {
                            xRChart = createChart("User authorizations", DataProcessing.userAuthorizationsDataTable);
                            addSingleSeriesToChart(xRChart, "User authorizations", ViewType.Bar, "VaultAuthorization", "Users", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.usersByLogRetentionPeriod))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users by audit log retention", headerLabel);

                        try
                        {
                            xRChart = createChart("Users by audit log retention", DataProcessing.usersByLogRetentionPeriod);
                            addSingleSeriesToChart(xRChart, "Users by audit log retention", ViewType.Bar, "AuditLogRetentionPeriod", "Users", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.groupsByOrigin))
                    {
                        subHeaderLabel = createSubHeaderLabel("Groups by origin", headerLabel);

                        try
                        {
                            xRChart = createChart("Groups by origin (%)", DataProcessing.groupsByOrigin);
                            addSingleSeriesToChart(xRChart, "Groups by origin (%)", ViewType.Pie, "Origin", "Groups", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            xRChart.PaletteName = "Solstice";

                            xRChart = createChart("Groups by origin", DataProcessing.groupsByOrigin);
                            addSingleSeriesToChart(xRChart, "Groups by origin", ViewType.Bar, "Origin", "Groups", true, true, "{V} groups");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.groupsByLDAP))
                    {
                        subHeaderLabel = createSubHeaderLabel("Groups by LDAP directory", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Groups by LDAP directory (%)", obfuscateDataTable(DataProcessing.groupsByLDAP, false, "LDAP directory"));
                            }
                            else
                            {
                                xRChart = createChart("Groups by LDAP directory (%)", DataProcessing.groupsByLDAP);
                            }


                            addSingleSeriesToChart(xRChart, "Groups by LDAP directoryn (%)", ViewType.Pie, "LDAPDirectory", "Groups", true, false);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                            xRChart.PaletteName = "Solstice";

                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Groups by LDAP directory", obfuscateDataTable(DataProcessing.groupsByLDAP, false, "LDAP directory"));
                            }
                            else
                            {
                                xRChart = createChart("Groups by LDAP directory", DataProcessing.groupsByLDAP);
                            }

                            addSingleSeriesToChart(xRChart, "Groups by LDAP directory", ViewType.Bar, "LDAPDirectory", "Groups", true, true, "{V} groups");
                            xRPanel = createXRPanelAndAddControl(null, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.builtinGroupMembers))
                    {
                        subHeaderLabel = createSubHeaderLabel("Members of builtin groups", headerLabel);

                        try
                        {
                            xRChart = createChart("Members of builtin groups", DataProcessing.builtinGroupMembers);
                            addSingleSeriesToChart(xRChart, "Members of builtin groups", ViewType.Bar, "Group", "UserMembers", true, true, "{V} users");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersDevelopmentDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users development", headerLabel);

                        try
                        {
                            xRChart = createChart("Users development", DataProcessing.usersDevelopmentDataTable);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersDevelopmentByUserTypeDataTable))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users development by user type", headerLabel);

                        try
                        {
                            xRChart = createChart("Users development by user type - Top 5", DataProcessing.usersDevelopmentByUserTypeDataTable);
                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date", 1, 5);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersDevelopmentByMapping))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users development by mapping", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users development by mapping", obfuscateDataTable(DataProcessing.usersDevelopmentByMapping, true, "Mapping"));
                            }
                            else
                            {
                                xRChart = createChart("Users development by mapping", DataProcessing.usersDevelopmentByMapping);
                            }

                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.usersDevelopmentByLDAP))
                    {
                        subHeaderLabel = createSubHeaderLabel("Users development by LDAP directory", headerLabel);

                        try
                        {
                            if (obfuscationLevel > 0)
                            {
                                xRChart = createChart("Users development by LDAP directory", obfuscateDataTable(DataProcessing.usersDevelopmentByLDAP, true, "LDAP directory"));
                            }
                            else
                            {
                                xRChart = createChart("Users development by LDAP directory", DataProcessing.usersDevelopmentByLDAP);
                            }

                            addMultipleSeriesToChart(xRChart, ViewType.Line, "Date");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }
                }

                if (isNotNullorEmpty(DataProcessing.permissionStatistics))
                {
                    headerLabel = createHeaderLabel("Permission management");
                    detailBand.SubBands.Add(addControlToSubBand(headerLabel, 40, false, true));
                    subHeaderLabel = createSubHeaderLabel("Permissions statistics", headerLabel);

                    try
                    {
                        xRTable = CreateXRTable(DataProcessing.permissionStatistics, 0);
                        xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRTable);
                        detailBand.SubBands.Add(addControlToSubBand(xRPanel, -1, true));
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                        Console.WriteLine(ex.Message);
                    }

                    if (isNotNullorEmpty(DataProcessing.epvUsersWithListAccess))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users with list access on % of all accounts", headerLabel);

                        try
                        {
                            xRChart = createChart("EPV users with list access", DataProcessing.epvUsersWithListAccess);
                            addSingleSeriesToChart(xRChart, "EPV users with list access on accounts", ViewType.Bar, "List accounts access on % of all accounts", "EPV Users", true, true, "{V} EPV user(s)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.epvUsersWithListRetrieveAccess))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users with list + retrieve password access on % of all accounts", headerLabel);

                        try
                        {
                            xRChart = createChart("EPV users with list + retrieve password access", DataProcessing.epvUsersWithListRetrieveAccess);
                            addSingleSeriesToChart(xRChart, "EPV users with list + retrieve password access", ViewType.Bar, "List + retrieve password access on % of all accounts", "EPV Users", true, true, "{V} EPV user(s)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.epvUsersWithListRetrieveNoConfirmAccess))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users with list + retrieve + w/o confirmation access on % of all accounts", headerLabel);

                        try
                        {
                            xRChart = createChart("EPV users with list + retrieve + w/o confirmation access", DataProcessing.epvUsersWithListRetrieveNoConfirmAccess);
                            addSingleSeriesToChart(xRChart, "EPV users with list + retrieve + w/o confirmation access", ViewType.Bar, "List + retrieve + w/o confirmation access on % of all accounts", "EPV Users", true, true, "{V} EPV user(s)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.epvUsersWithListUseAccess))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users with list + use password access on % of all accounts", headerLabel);

                        try
                        {
                            xRChart = createChart("EPV users with list + use password access", DataProcessing.epvUsersWithListUseAccess);
                            addSingleSeriesToChart(xRChart, "EPV users with list + use password access", ViewType.Bar, "List + use password on % of all accounts", "EPV Users", true, true, "{V} EPV user(s)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.epvUsersWithListUseNoConfirmAccess))
                    {
                        subHeaderLabel = createSubHeaderLabel("EPV users with list + use + w/o confirmation on % of all accounts", headerLabel);

                        try
                        {
                            xRChart = createChart("EPV users with list + use + w/o confirmation access", DataProcessing.epvUsersWithListUseNoConfirmAccess);
                            addSingleSeriesToChart(xRChart, "EPV users with list + use + w/o confirmation access", ViewType.Bar, "List + use + w/o confirmation on % of all accounts", "EPV Users", true, true, "{V} EPV user(s)");
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.aamProviderPermissions))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts per AAM provider", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Accounts per AAM provider - Top 10", obfuscateDataTable(DataProcessing.aamProviderPermissions, false, "AAM provider"));
                            }
                            else
                            {
                                xRChart = createChart("Accounts per AAM provider - Top 10", DataProcessing.aamProviderPermissions);
                            }

                            addSingleSeriesToChart(xRChart, "Accounts per AAM provider - Top 10", ViewType.Bar, "AAM Provider", "Accounts", true, true, "{V} accounts", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }

                    if (isNotNullorEmpty(DataProcessing.aamApplicationPermissions))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts per AAM application", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Accounts per AAM application - Top 10", obfuscateDataTable(DataProcessing.aamApplicationPermissions, false, "AAM application"));
                            }
                            else
                            {
                                xRChart = createChart("Accounts per AAM application - Top 10", DataProcessing.aamApplicationPermissions);
                            }

                            addSingleSeriesToChart(xRChart, "Accounts per AAM application - Top 10", ViewType.Bar, "AAM Application", "Accounts", true, true, "{V} accounts", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }


                    if (isNotNullorEmpty(DataProcessing.opmAgentPermissions))
                    {
                        subHeaderLabel = createSubHeaderLabel("Accounts per OPM agent", headerLabel);

                        try
                        {

                            if (obfuscationLevel > 1)
                            {
                                xRChart = createChart("Accounts per OPM agent - Top 10", obfuscateDataTable(DataProcessing.opmAgentPermissions, false, "OPM agent"));
                            }
                            else
                            {
                                xRChart = createChart("Accounts per OPM agent - Top 10", DataProcessing.opmAgentPermissions);
                            }

                            addSingleSeriesToChart(xRChart, "Accounts per OPM agent - Top 10", ViewType.Bar, "OPM Agent", "Accounts", true, true, "{V} accounts", null, 10);
                            xRPanel = createXRPanelAndAddControl(subHeaderLabel, xRChart);
                            detailBand.SubBands.Add(addControlToSubBand(xRPanel));
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(@"An error occurred while trying to create report items in section """ + subHeaderLabel.Text + @"""");
                            Console.WriteLine(ex.Message);
                        }
                    }



                }





                int firstLevelNumber = 0;
                int secondLevelNumber = 0;
                foreach (XRControl control in environmentReport.AllControls<XRControl>())
                {
                    if (control.GetType() == typeof(XRLabel) && ((XRLabel)control).Bookmark != null && ((XRLabel)control).Bookmark.ToString() != string.Empty && ((XRLabel)control).Text != null && ((XRLabel)control).Text != string.Empty)
                    {
                        XRLabel xRLabel = (XRLabel)control;
                        if (xRLabel.BookmarkParent == null)
                        {
                            firstLevelNumber++;
                            secondLevelNumber = 0;
                            xRLabel.Bookmark = firstLevelNumber + ". " + xRLabel.Bookmark;
                            xRLabel.Text = firstLevelNumber + ". " + xRLabel.Text;
                        }
                        else
                        {
                            secondLevelNumber++;
                            xRLabel.Bookmark = firstLevelNumber + "." + secondLevelNumber + " " + xRLabel.Bookmark;
                            xRLabel.Text = firstLevelNumber + "." + secondLevelNumber + " " + xRLabel.Text;
                        }
                    }
                }

                environmentReport.CreateDocument();
                environmentReport.PrintingSystem.Document.AutoFitToPagesWidth = 1;
                if (environmentReport.Pages.Count < 4)
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occured while trying to create environment report");
                Console.WriteLine("Error: " + ex.Message);
                if (ex.InnerException != null)
                {
                    Console.WriteLine(ex.InnerException.Message);
                }
                return null;
            }
            finally
            {
                reportIsGettingGenerated = false;
            }

            return environmentReport;
        }

        private static DataTable obfuscateDataTable(DataTable table, bool tableRequiresolumnNameObfuscation = false, string objfuscationString = "", int obfuscationColumnNumber = 0, bool tableRequiresColumnsRemoval = false, int startColumn = 1, int endColumn = -1)
        {
            DataTable obfucscatedTable = table.Copy();
            int counter = 1;
            if (tableRequiresolumnNameObfuscation)
            {
                if (endColumn == -1)
                {
                    endColumn = obfucscatedTable.Columns.Count - 1;
                }
                for (int i = startColumn; i <= endColumn; i++)
                {
                    if (objfuscationString != "Server ID")
                    {
                        obfucscatedTable.Columns[i].ColumnName = objfuscationString + " " + (counter).ToString("D3");

                    }
                    else if (obfucscatedTable.Columns[i].ColumnName.Contains("("))
                    {
                        obfucscatedTable.Columns[i].ColumnName = objfuscationString + " " + (counter).ToString("D3") + " (" + obfucscatedTable.Columns[i].ColumnName.Split('(')[1];
                    }
                    counter++;
                }
            }
            else
            {
                for (int i = 0; i < obfucscatedTable.Rows.Count; i++)
                {
                    if (objfuscationString != "Server ID")
                    {
                        obfucscatedTable.Rows[i][obfuscationColumnNumber] = objfuscationString + " " + (counter).ToString("D3");
                    }
                    else if (obfucscatedTable.Rows[i][obfuscationColumnNumber].ToString().Contains('('))
                    {
                        obfucscatedTable.Rows[i][obfuscationColumnNumber] = objfuscationString + " " + (counter).ToString("D3") + " (" + obfucscatedTable.Rows[i][obfuscationColumnNumber].ToString().Split('(')[1];
                    }
                    counter++;
                }
            }

            return obfucscatedTable;
        }

        private static void XRControl_OnlyPrintOnFirstPage(object sender, PrintOnPageEventArgs e)
        {
            if (e.PageIndex > 0)
            {
                e.Cancel = true;
            }
        }
        private static void xRPageInfo_PrintOnPage(object sender, PrintOnPageEventArgs e)
        {
            if (e.PageIndex < 3)
            {
                e.Cancel = true;
            }
        }
        public static XRPanel createXRPanelAndAddControl(XRControl xRControl1, XRControl xRControl2, int height1 = -1, int height2 = -1)
        {
            XRTable xRTable = new XRTable();
            XRTableCell xRTableCell1 = new XRTableCell();
            XRTableCell xRTableCell2 = new XRTableCell();
            XRTableRow xRTableRow1 = new XRTableRow();
            XRTableRow xRTableRow2 = new XRTableRow();

            if (xRControl1 != null)
            {
                xRTableRow1.Cells.Add(xRTableCell1);
                xRTable.Rows.Add(xRTableRow1);
                xRTableCell1.Controls.Add(xRControl1);
            }

            if (xRControl2 != null)
            {
                xRTableRow2.Cells.Add(xRTableCell2);
                xRTable.Rows.Add(xRTableRow2);
                xRTableCell2.Controls.Add(xRControl2);
            }

            XRPanel xRPanel = new XRPanel();
            xRPanel.Controls.Add(xRTable);

            if (xRControl1 != null)
            {
                if (height1 != -1)
                {
                    xRControl1.HeightF = height1;
                }
                else if (xRControl1.GetType() == typeof(XRLabel))
                {
                    xRControl1.HeightF = 45;
                }
            }

            if (xRControl2 != null)
            {
                if (height2 != -1)
                {
                    xRControl2.HeightF = height2;
                }
                else if (xRControl2.GetType() == typeof(XRChart))
                {
                    if (xRControl1 != null)
                    {
                        xRControl2.HeightF = 400;
                    }
                    else
                    {
                        xRControl2.HeightF = 420;
                    }
                }
            }

            xRTable.BeforePrint += XRControl_BeforePrint;
            xRPanel.BeforePrint += XRControl_BeforePrint;
            return xRPanel;
        }

        public static XRPanel createXRPanel()
        {
            XRPanel xRPanel = new XRPanel();
            xRPanel.BeforePrint += XRControl_BeforePrint;
            return xRPanel;
        }


        private static float getPageCenterXCoordinate(XtraReport xtraReport, XRControl xRControl)
        {
            return ((xtraReport.PageWidth - xtraReport.Margins.Left) / 2 - (xRControl.WidthF / 2));
        }

        private static XRTable resizeColumnsInTable(XRTable xRTable, int columnToResize = 0, bool cutColumnWithToHalf = false, int columnWidth = 10)
        {
            for (int i = 0; i < xRTable.Rows.Count; i++)
            {
                XRTableRow row = xRTable.Rows[i];
                XRTableCell editedCell = row.Cells[columnToResize];
                int editedCellWidth = editedCell.Width;
                if (cutColumnWithToHalf)
                {
                    editedCell.Width -= editedCellWidth / 2;
                }
                else
                {
                    editedCell.Width = columnWidth;
                }
            }
            return xRTable;
        }
        private static XRTable CreateXRTable(DataTable dataTable, int startColumn, int endColumn = -1, string bookMark = "", XRControl parentBookMark = null)
        {
            XRTable table = new XRTable();
            table.Borders = BorderSide.All;
            table.BorderWidth = 1;

            if (endColumn == -1)
            {
                endColumn = dataTable.Columns.Count - 1;
            }

            table.BeginInit();

            XRTableRow headerRow = new XRTableRow();
            for (int i = startColumn; i <= endColumn; i++)
            {
                XRTableCell cell = new XRTableCell();
                cell.Text = dataTable.Columns[i].ColumnName;
                headerRow.Cells.Add(cell);
            }
            table.Rows.Add(headerRow);

            for (int j = 0; j < dataTable.Rows.Count; j++)
            {
                XRTableRow row = new XRTableRow();
                for (int i = startColumn; i <= endColumn; i++)
                {
                    XRTableCell cell = new XRTableCell();
                    cell.Text = dataTable.Rows[j][i].ToString();
                    row.Cells.Add(cell);
                }
                table.Rows.Add(row);
            }

            table.BeforePrint += XRControl_BeforePrint;
            table.AdjustSize();
            table.EndInit();
            return table;
        }

        static SubBand addControlToSubBand(XRControl xRControl, float height = -1, bool addPageBreakAfterBand = false, bool addPageBreakBeforeBand = false)
        {
            SubBand subBand = new SubBand();
            if (height != -1)
            {
                subBand.HeightF = height;

                if (xRControl.GetType() == typeof(XRChart))
                {
                    xRControl.HeightF = height;
                }

            }
            if (addPageBreakAfterBand)
            {
                subBand.PageBreak = PageBreak.AfterBand;
            }
            if (addPageBreakBeforeBand)
            {
                subBand.PageBreak = PageBreak.BeforeBand;
            }
            subBand.Controls.Add(xRControl);
            return subBand;
        }

        static SubBand addMarginSubBand(int height = -1)
        {
            SubBand subBand = new SubBand();
            if (height != -1)
            {
                subBand.HeightF = height;
            }
            else
            {
                subBand.HeightF = 50;
            }

            subBand.Controls.Add(new XRControl() { HeightF = 0, WidthF = 0 });

            return subBand;
        }
        static XRLabel createHeaderLabel(string labelText, string bookMark = "", int labelWidth = -1)
        {
            XRLabel xRLabel = new XRLabel();
            xRLabel.Text = labelText;
            xRLabel.Font = new Font("Calibri", 16);
            xRLabel.ForeColor = ColorTranslator.FromHtml("#2F5496");

            if (labelWidth != -1)
            {
                xRLabel.WidthF = labelWidth;
            }
            else
            {
                xRLabel.BeforePrint += XRControl_BeforePrint;
            }

            if (bookMark != "")
            {
                xRLabel.Bookmark = bookMark;
            }
            else
            {
                xRLabel.Bookmark = labelText;
            }

            return xRLabel;
        }

        static XRLabel createSubHeaderLabel(string labelText, XRControl parentBookmark = null, string bookMark = "", int labelWidth = -1)
        {
            XRLabel xRLabel = new XRLabel();
            xRLabel.Text = labelText;
            xRLabel.WidthF = labelWidth;
            xRLabel.Font = new Font("Calibri", 14, FontStyle.Underline);
            xRLabel.ForeColor = ColorTranslator.FromHtml("#2F5496");
            if (labelWidth != -1)
            {
                xRLabel.WidthF = labelWidth;
            }
            else
            {
                xRLabel.BeforePrint += XRControl_BeforePrint;
            }

            if (bookMark != "")
            {
                xRLabel.Bookmark = bookMark;
            }
            else
            {
                xRLabel.Bookmark = labelText;
            }
            if (parentBookmark != null)
            {
                xRLabel.BookmarkParent = parentBookmark;
            }

            return xRLabel;
        }

        private static float getReportWidthWithoutMargins()
        {
            return environmentReport.PageWidth - environmentReport.Margins.Left - environmentReport.Margins.Right;
        }


        private static XRChart createChart(string chartName, DataTable dataSource, bool showLegend = true, string bookMarkName = null, XRControl bookMarkParent = null, float width = -1, Legend legend = null)
        {
            XRChart xRChart = new XRChart();
            xRChart.Titles.Add(new ChartTitle { Text = chartName });
            xRChart.Name = chartName;
            xRChart.DataSource = dataSource;

            if (!showLegend)
            {
                xRChart.Legend.Visibility = DevExpress.Utils.DefaultBoolean.False;
            }

            if (legend == null)
            {
                xRChart.Legend.AlignmentVertical = LegendAlignmentVertical.TopOutside;
                xRChart.Legend.AlignmentHorizontal = LegendAlignmentHorizontal.Center;
                xRChart.Legend.Direction = LegendDirection.LeftToRight;
                xRChart.Legend.MaxHorizontalPercentage = 100;
            }

            if (width != -1)
            {
                xRChart.WidthF = width;
            }
            else
            {
                xRChart.BeforePrint += XRControl_BeforePrint;
            }

            if (bookMarkName != null)
            {
                xRChart.Bookmark = bookMarkName;

                if (bookMarkParent != null)
                {
                    xRChart.BookmarkParent = bookMarkParent;
                }
            }

            return xRChart;
        }

        static void PageHeaderTable_BeforePrint(object sender, PrintEventArgs e)
        {
            XRControl xRControl = ((XRControl)sender);
            xRControl.LocationF = new DevExpress.Utils.PointFloat(0, 47F);
            xRControl.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }
        static void XRControl_BeforePrint(object sender, PrintEventArgs e)
        {
            XRControl xRControl = ((XRControl)sender);
            xRControl.LocationF = new DevExpress.Utils.PointFloat(0F, 0F);
            xRControl.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }

        static void label_BeforePrint(object sender, PrintEventArgs e)
        {
            XRLabel label = ((XRLabel)sender);
            label.LocationF = new DevExpress.Utils.PointFloat(0F, 0F);
            label.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }


        static void table_BeforePrint(object sender, PrintEventArgs e)
        {
            XRTable table = ((XRTable)sender);
            table.LocationF = new DevExpress.Utils.PointFloat(0F, 0F);
            table.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }

        static void XRTableOfContents_BeforePrint(object sender, PrintEventArgs e)
        {
            XRTableOfContents toc = ((XRTableOfContents)sender);
            toc.LocationF = new DevExpress.Utils.PointFloat(0F, 0F);
            toc.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }
        static void XRChart_BeforePrint(object sender, PrintEventArgs e)
        {
            XRChart chart = ((XRChart)sender);
            chart.LocationF = new DevExpress.Utils.PointFloat(0F, 0F);
            chart.WidthF = environmentReport.PageWidth - environmentReport.Margins.Right;
        }

        private static bool isNotNullorEmpty(DataTable table)
        {
            if (table != null && table.Rows.Count > 0 && table.Columns.Count > 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }


        private static void addSingleSeriesToChart(XRChart chart, string seriesName, ViewType viewType, string argumentColumnName, string valueColumnName, bool labelsVisibility = false, bool rotated = false, string labelTextPattern = null, string legendTextPattern = null, int topNOptions = -1, bool markerVisibility = false)
        {

            Series series = new Series(seriesName, viewType);


            if (labelsVisibility)
            {
                series.LabelsVisibility = DevExpress.Utils.DefaultBoolean.True;

                if (viewType == ViewType.Pie && labelTextPattern == null)
                {
                    series.Label.TextPattern = "{VP:P}";
                }
                else if (labelTextPattern != null)
                {
                    series.Label.TextPattern = labelTextPattern;
                }

                series.Label.ResolveOverlappingMode = ResolveOverlappingMode.Default;
            }
            else
            {
                series.LabelsVisibility = DevExpress.Utils.DefaultBoolean.False;
            }


            DataTable dataSource = (DataTable)chart.DataSource;
            series.DataSource = dataSource;
            series.ArgumentDataMember = dataSource.Columns[argumentColumnName].Caption;
            series.ValueDataMembers[0] = dataSource.Columns[valueColumnName].Caption;

            if (viewType == ViewType.Pie)
            {
                if (legendTextPattern == null)
                {
                    series.LegendTextPattern = "{A}: {VP:P}";
                }
            }

            if (legendTextPattern != null)
            {
                series.LegendTextPattern = legendTextPattern;
            }

            if (topNOptions > 1)
            {
                series.TopNOptions.Mode = TopNMode.Count;
                series.TopNOptions.Enabled = true;
                series.TopNOptions.Count = topNOptions;

                if (viewType == ViewType.Pie)
                {
                    series.TopNOptions.ShowOthers = true;
                }
                else
                {
                    series.TopNOptions.ShowOthers = false;
                }
            }

            chart.Series.Add(series);

            if (viewType == ViewType.Bar || viewType == ViewType.StackedBar || viewType == ViewType.Area)
            {
                series.ShowInLegend = false;

                if (rotated)
                {
                    ((XYDiagram)chart.Diagram).Rotated = true;
                }
                if (labelsVisibility)
                {
                    BarSeriesLabelPosition barSeriesLabelPosition = new BarSeriesLabelPosition();
                    barSeriesLabelPosition = BarSeriesLabelPosition.Top;
                    ((BarSeriesLabel)series.Label).Position = barSeriesLabelPosition;
                }
            }

            if (viewType == ViewType.Line || viewType == ViewType.StackedBar || viewType == ViewType.Area)
            {
                if (markerVisibility)
                {
                    if (viewType == ViewType.Line)
                    {
                        ((LineSeriesView)chart.Series[seriesName].View).MarkerVisibility = DevExpress.Utils.DefaultBoolean.True;
                    }
                    else if (viewType == ViewType.Area)
                    {
                        ((AreaSeriesView)chart.Series[seriesName].View).MarkerVisibility = DevExpress.Utils.DefaultBoolean.True;
                    }
                }
                else
                {
                    if (viewType == ViewType.Line)
                    {
                        ((LineSeriesView)chart.Series[seriesName].View).MarkerVisibility = DevExpress.Utils.DefaultBoolean.False;
                    }
                    else if (viewType == ViewType.Area)
                    {
                        ((AreaSeriesView)chart.Series[seriesName].View).MarkerVisibility = DevExpress.Utils.DefaultBoolean.False;
                    }
                }

                if (viewType == ViewType.Line)
                {
                    ((LineSeriesView)chart.Series[seriesName].View).LineStyle.Thickness = 3;
                }
            }


            if (chart.Diagram.GetType() == typeof(XYDiagram))
            {
                ((XYDiagram)chart.Diagram).AxisX.QualitativeScaleOptions.AutoGrid = false;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowRotate = true;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowHide = true;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowStagger = true;
            }

        }

        private static void addMultipleSeriesToChart(XRChart chart, ViewType viewType, string argumentColumnName, int valueColumnStartNumber = 1, int maximumNumberOfSeries = -1, DevExpress.Utils.DefaultBoolean labelsVisibility = DevExpress.Utils.DefaultBoolean.False, DevExpress.Utils.DefaultBoolean markerVisibility = DevExpress.Utils.DefaultBoolean.False, DataTable inputDataTable = null)
        {
            DataTable dataSource = (DataTable)chart.DataSource;

            if (maximumNumberOfSeries == -1 || maximumNumberOfSeries > dataSource.Columns.Count - 1)
            {
                maximumNumberOfSeries = dataSource.Columns.Count - 1;
            }


            for (int i = valueColumnStartNumber; i <= maximumNumberOfSeries; i++)
            {
                Series series = new Series("", viewType);
                series.LabelsVisibility = labelsVisibility;
                series.Name = dataSource.Columns[i].ColumnName;
                series.DataSource = dataSource;
                series.ArgumentDataMember = dataSource.Columns[argumentColumnName].Caption;
                series.ValueDataMembers[0] = dataSource.Columns[i].Caption;
                chart.Series.Add(series);

                if (viewType == ViewType.Line)
                {
                    ((LineSeriesView)chart.Series[i - valueColumnStartNumber].View).MarkerVisibility = markerVisibility;
                    ((LineSeriesView)chart.Series[i - valueColumnStartNumber].View).LineStyle.Thickness = 3;
                }
            }


            if (chart.Diagram.GetType() == typeof(XYDiagram))
            {
                ((XYDiagram)chart.Diagram).AxisX.QualitativeScaleOptions.AutoGrid = false;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowRotate = true;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowHide = true;
                ((XYDiagram)chart.Diagram).AxisX.Label.ResolveOverlappingOptions.AllowStagger = true;
            }

        }

        private static bool checkIfHasManagedAccounts()
        {
            if (DataProcessing.managedAccountsDataTable != null && DataProcessing.managedAccountsDataTable.Rows.Count > 0 && Double.TryParse(DataProcessing.managedAccountsDataTable.Rows[0][1].ToString(), out double managedAccounts) && managedAccounts > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }



        public class reportExportJob
        {
            public int exportJobID { get; set; }
            public int reportID { get; set; }
            public string reportFormat { get; set; }
            public string exportFolder { get; set; }
            public List<int> weekdaysToExportReport { get; set; }
            public bool exportOnlyOncePerDay { get; set; }
            public string lastExportDate { get; set; }

            public static int processedExportJobs = 0;

            public reportExportJob() { }
            public reportExportJob(int jobID, int reportID, string format, string folder, List<int> weekdays, bool exportOnlyOnce)
            {
                this.exportJobID = jobID;
                this.reportID = reportID;
                this.reportFormat = format;
                this.exportFolder = folder;
                this.weekdaysToExportReport = weekdays;
                this.exportOnlyOncePerDay = exportOnlyOnce;
            }

            public static int generateExportJobID(List<Report.reportExportJob> exportJobList)
            {
                if (exportJobList == null || exportJobList.Count == 0)
                {
                    return 0;
                }
                else
                {
                    return exportJobList.Max(job => job.exportJobID) + 1;
                }
            }

            public static void processExportJobs(bool testMode = false, reportExportJob reportExportJob = null)
            {
                try
                {
                    processedExportJobs = 0;
                    settings = MySettings.Load();
                    if (testMode || !MainWindow.onlyTraces && (settings.AutomaticReportExport == 1 || (settings.AutomaticReportExport == 2 && MainWindow.automaticMode)) && settings.reportExportJobsList != null && settings.reportExportJobsList.Count > 0 && File.Exists(@"data\PASReporter.db"))
                    {
                        if (!testMode)
                        {
                            Console.WriteLine(DateTime.Now + " Starting to process export jobs...");
                        }

                        List<int> reportIDs = new List<int>();

                        if (!testMode)
                        {
                            reportIDs = settings.reportExportJobsList.Select(x => x.reportID).ToList().Distinct().ToList();
                        }
                        else if (reportExportJob != null)
                        {
                            reportIDs.Add(reportExportJob.reportID);
                        }

                        List<XtraReport> reportList = new List<XtraReport>();
                        foreach (int ID in reportIDs)
                        {
                            XtraReport report = Report.createReportFromID(ID);
                            if (report != null)
                            {
                                reportList.Add(report);
                            }
                            else
                            {
                                if (!testMode)
                                {
                                    Console.WriteLine(DateTime.Now + @" Warning: Export jobs with the report """ + MainWindow.reportsList[ID].displayName + @""" will not be processed. Reason: The report has not been created.");
                                }
                                else
                                {
                                    MessageBox.Show("Error: The export job cannot be processed because the required report has not been created." + Environment.NewLine + Environment.NewLine + "Please make sure to process required data with the tool so that the report can be created.", "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                                    return;
                                }
                            }
                        }

                        if (reportList != null && reportList.Count > 0)
                        {
                            if (!testMode)
                            {
                                List<string> reportTags = reportList.Select(x => x.Tag.ToString()).ToList().Distinct().ToList();

                                foreach (reportExportJob exportJob in settings.reportExportJobsList)
                                {
                                    if (reportTags.Contains(exportJob.reportID.ToString()))
                                    {

                                        if (checkIfExportJobCanBeProcessedOnThisWeekday(exportJob))
                                        {
                                            if (checkIfExportJobCanBeProcessedToday(exportJob))
                                            {
                                                processExportJob(exportJob, reportList.First(a => (string)a.Tag == exportJob.reportID.ToString()));
                                            }
                                            else if (settings.detailedEmailReportLogging)
                                            {
                                                Console.WriteLine(DateTime.Now + " Information: The processing of the export job with the ID " + exportJob.exportJobID + " will be skipped because the export job has already been processed today and the export job is configured to be exported only once per day.");
                                            }
                                        }
                                        else if (settings.detailedEmailReportLogging)
                                        {
                                            Console.WriteLine(DateTime.Now + " Information: The processing of the export job with the ID " + exportJob.exportJobID + " will be skipped because the export job is configured to NOT be exported on today's weekday (" + DateTime.Now.DayOfWeek + ").");
                                        }
                                    }
                                }
                            }
                            else
                            {
                                processExportJob(reportExportJob, reportList.First(a => (string)a.Tag == reportExportJob.reportID.ToString()), testMode);
                            }
                        }
                        if (!testMode)
                        {
                            Console.WriteLine(DateTime.Now + " Finished processing report export jobs");
                            Console.WriteLine(DateTime.Now + " " + processedExportJobs + " out of " + settings.reportExportJobsList.Count + " export jobs were processed");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (!testMode)
                    {
                        Console.WriteLine(DateTime.Now + " An error occurred while trying to process report export jobs:  " + ex.Message);
                    }
                    else
                    {
                        MessageBox.Show("An error occurred while trying to process the report email job." + Environment.NewLine + Environment.NewLine + ex.Message, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                        return;
                    }
                }
                finally
                {
                    if (!testMode)
                    {
                        Console.Write(Environment.NewLine);
                    }
                }
            }

            public static void processExportJob(reportExportJob exportJob, XtraReport report, bool testMode = false)
            {
                try
                {
                    DateTime dateTime;
                    if (!DateTime.TryParseExact(report.Name.Split('_')[0].ToString(), "yyyy-MM-dd hh:mm tt", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out dateTime))
                    {
                        dateTime = DateTime.Now;
                    }

                    string fileName = report.Name.Replace(report.Name.Split('_')[0].ToString(), "").TrimStart('_');
                    string folderName = exportJob.exportFolder;

                    if (settings.useSubfoldersForReportExport)
                    {
                        folderName = Path.Combine(folderName, fileName);
                        Directory.CreateDirectory(folderName);
                    }

                    if (settings.addReportCreationDateToReport)
                    {
                        if (settings.addReportCreationDateOption == 0)
                        {
                            fileName = dateTime.ToString("yyyy-MM-dd_hh-mm") + "_" + fileName;
                        }
                        else if (settings.addReportCreationDateOption == 1)
                        {
                            fileName += "_" + dateTime.ToString("yyyy-MM-dd_hh-mm");
                        }
                    }

                    folderName = Path.Combine(folderName, fileName);

                    report.CreateDocument(false);
                    report.PrintingSystem.Document.AutoFitToPagesWidth = 1;

                    if (exportJob.reportFormat.ToUpper().Trim() == "XLSX" || exportJob.reportFormat.ToUpper().Trim() == "HTMLFILE")
                    {
                     
                            XRControl control = report.FindControl("PageNumberInfo", true);
                            if (control != null)
                            {
                                control.CanPublish = false;
                            }
                        
                    }

                    if (exportJob.reportFormat.ToUpper().Trim() == "PDF")
                    {
                        PdfExportOptions options = new PdfExportOptions();
                        options.DocumentOptions.Author = MainWindow.PASReporterVersionLong;
                        options.ImageQuality = PdfJpegImageQuality.Medium;

                        folderName += ".pdf";

                        try
                        {
                            using (FileStream filestream = new FileStream(folderName, FileMode.Create))
                            {
                                report.ExportToPdf(filestream, options);
                            }

                        }
                        catch (Exception ex)
                        {
                            if (!testMode)
                            {
                                Console.WriteLine(DateTime.Now + " Failed to process export job with ID" + exportJob.exportJobID);
                                Console.WriteLine(DateTime.Now + " Error: " + ex.Message);
                            }
                            else
                            {
                                string errorText = "An error occurred while trying to process the export job." + Environment.NewLine + Environment.NewLine + ex.Message;
                                MessageBox.Show(errorText, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                            }
                            return;
                        }
                    }
                    else if (exportJob.reportFormat.ToUpper().Trim() == "DOCX")
                    {
                        DocxExportOptions docxExportOptions = new DocxExportOptions();
                        docxExportOptions.DocumentOptions.Author = MainWindow.PASReporterVersionLong;

                        folderName += ".docx";

                        try
                        {
                            using (FileStream filestream = new FileStream(folderName, FileMode.Create))
                            {
                                report.ExportToDocx(filestream, docxExportOptions);
                            }
                        }
                        catch (Exception ex)
                        {
                            if (!testMode)
                            {
                                Console.WriteLine(DateTime.Now + " Failed to process export job with ID" + exportJob.exportJobID);
                                Console.WriteLine(DateTime.Now + " Error: " + ex.Message);
                            }
                            else
                            {
                                string errorText = "An error occurred while trying to process the export job." + Environment.NewLine + Environment.NewLine + ex.Message;
                                MessageBox.Show(errorText, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                            }
                            return;
                        }



                    }
                    else if (exportJob.reportFormat.ToUpper().Trim() == "XLSX")
                    {
                        XlsxExportOptions xlsExportOptions = new XlsxExportOptions();
                        if (report.DisplayName.ToLower().Contains("obfuscated") || report.DisplayName.ToLower().Contains("obfuscation"))
                        {
                            xlsExportOptions.SheetName = report.DisplayName.Split(' ')[0] + " " + report.DisplayName.Split(' ')[1] + " (obfuscated)");
                        }
                        else
                        {
                            xlsExportOptions.SheetName = report.DisplayName;
                        }
                        xlsExportOptions.DocumentOptions.Author = MainWindow.PASReporterVersionLong;

                        folderName += ".xlsx";

                        try
                        {
                            using (FileStream filestream = new FileStream(folderName, FileMode.Create))
                            {
                                report.ExportToXlsx(filestream, xlsExportOptions);
                            }
                        }
                        catch (Exception ex)
                        {
                            if (!testMode)
                            {
                                Console.WriteLine(DateTime.Now + " Failed to process export job with ID" + exportJob.exportJobID);
                                Console.WriteLine(DateTime.Now + " Error: " + ex.Message);
                            }
                            else
                            {
                                string errorText = "An error occurred while trying to process the export job." + Environment.NewLine + Environment.NewLine + ex.Message;
                                MessageBox.Show(errorText, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                            }
                            return;
                        }

                    }
                    else if (exportJob.reportFormat.ToUpper().Trim() == "HTMLFILE")
                    {
                        HtmlExportOptions htmlExportOptions = new HtmlExportOptions();
                        htmlExportOptions.EmbedImagesInHTML = true;
                        htmlExportOptions.ExportMode = HtmlExportMode.SingleFile;
                        htmlExportOptions.Title = report.Name;
                        folderName += ".html";

                        try
                        {
                            using (FileStream filestream = new FileStream(folderName, FileMode.Create))
                            {
                                report.ExportToHtml(filestream, htmlExportOptions);
                            }
                        }
                        catch (Exception ex)
                        {
                            if (!testMode)
                            {
                                Console.WriteLine(DateTime.Now + " Failed to process export job with ID" + exportJob.exportJobID);
                                Console.WriteLine(DateTime.Now + " Error: " + ex.Message);
                            }
                            else
                            {
                                string errorText = "An error occurred while trying to process the export job." + Environment.NewLine + Environment.NewLine + ex.Message;
                                MessageBox.Show(errorText, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                            }
                            return;
                        }

                    }
                    else
                    {
                        Console.WriteLine(DateTime.Now + " Error: Export job export type " + exportJob.reportFormat + " is not supported");
                        return;
                    }

                    if (!testMode)
                    {
                        processedExportJobs += 1;
                        settings.reportExportJobsList.Where(x => x.exportJobID == exportJob.exportJobID).FirstOrDefault().lastExportDate = DateTime.Now.ToString("yyyy-MM-dd hh:mm tt");
                        settings.Save();
                        Console.WriteLine(DateTime.Now + " Export job with ID " + exportJob.exportJobID + " was successfully processed");
                    }
                    else
                    {
                        MessageBox.Show("The export job with the ID " + exportJob.exportJobID + " was saved and successfully processed", "Export job processing successful", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Information);
                    }
                }
                catch (Exception ex)
                {
                    if (!testMode)
                    {
                        Console.WriteLine(DateTime.Now + " Failed to process export job with ID" + exportJob.exportJobID);
                        Console.WriteLine(DateTime.Now + " Error: " + ex.Message);

                        if (ex.InnerException.Message != null)
                        {
                            Console.WriteLine(DateTime.Now + "Error: " + ex.InnerException.Message);
                        }
                    }
                    else
                    {
                        string errorText = "An error occurred while trying to process the export job." + Environment.NewLine + Environment.NewLine + ex.Message;

                        if (ex.InnerException.Message != null)
                        {
                            errorText += Environment.NewLine + ex.InnerException.Message;
                        }

                        MessageBox.Show(errorText, "Error", System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Error);
                    }
                }
            }

            private static bool checkIfExportJobCanBeProcessedToday(reportExportJob reportExportJob)
            {
                if (reportExportJob.exportOnlyOncePerDay == false || reportExportJob.lastExportDate == null)
                {
                    return true;
                }
                else if (reportExportJob.exportOnlyOncePerDay == true && reportExportJob.lastExportDate != null && ((DateTime.TryParse(reportExportJob.lastExportDate.ToString(), out DateTime exportDate)) && exportDate.Date < DateTime.Today))
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }

            private static bool checkIfExportJobCanBeProcessedOnThisWeekday(reportExportJob reportExportJob)
            {
                if (reportExportJob.weekdaysToExportReport == null || reportExportJob.weekdaysToExportReport.Count == 0)
                {
                    return false;
                }
                else if (reportExportJob.weekdaysToExportReport != null && reportExportJob.weekdaysToExportReport.Count == 7)
                {
                    return true;
                }
                else
                {
                    string weekday = DateTime.Now.DayOfWeek.ToString();

                    if (weekday == "Monday" && reportExportJob.weekdaysToExportReport.Contains(0))
                    {
                        return true;
                    }
                    else if (weekday == "Tuesday" && reportExportJob.weekdaysToExportReport.Contains(1))
                    {
                        return true;
                    }
                    else if (weekday == "Wednesday" && reportExportJob.weekdaysToExportReport.Contains(2))
                    {
                        return true;
                    }
                    else if (weekday == "Thursday" && reportExportJob.weekdaysToExportReport.Contains(3))
                    {
                        return true;
                    }
                    else if (weekday == "Friday" && reportExportJob.weekdaysToExportReport.Contains(4))
                    {
                        return true;
                    }
                    else if (weekday == "Saturday" && reportExportJob.weekdaysToExportReport.Contains(5))
                    {
                        return true;
                    }
                    else if (weekday == "Sunday" && reportExportJob.weekdaysToExportReport.Contains(6))
                    {
                        return true;
                    }
                }

                return false;
            }
        }
    }      					
					
					
					
					
					// Data processing logic
					// Determining safes and files information
                    numberOfSafes = safes.Count();
                    allSafesList = (from s in safes select s.SafeName).Distinct().ToList();

                    numberOfObjects = files.Count();

                    numberOfAccounts = files
                        .Where(f => f.Type == 2 && !f.SafeName.ToLower().Contains("_workspace") && f.SafeName.ToLower() != "psmunmanagedsessionaccounts")
                        .Select(f => f.Type)
                        .Count();

                    numberOfFiles = files
                        .Where(f => f.Type == 1 && !f.FileName.EndsWith(".session"))
                        .Select(f => f.Type)
                        .Count();

                    DateTime mintime = new DateTime(2005, 1, 1);
                    DateTime minimumObjectsCreationDate = files.Min(x => x.CreationDate);
                    DateTime minimumSafeCreationDate = safes.Min(x => x.CreationDate);


                    var datetimes = (from f in files where f.CreationDate.Year != 1 select f);
                   minimumObjectsCreationDate = datetimes.Min(x => x.CreationDate);
                
               
                    var datetimes = (from f in safes where f.CreationDate.Year != 1 select f);
                    minimumSafeCreationDate = datetimes.Min(x => x.CreationDate);

                    var deletedObjects = from f in files where f.DeletedBy != String.Empty select f;
                    numberOfDeletedObjects = deletedObjects.Count();

                    var deletedAccounts = from f in deletedObjects where f.Type == 2 && !f.SafeName.ToLower().EndsWith("_workspace") && f.SafeName.ToLower() != "psmunmanagedsessionaccounts" select f;
                    numberOfDeletedAccounts = deletedAccounts.Count();

                    var deletedFiles = from f in deletedObjects where f.Type == 1 && !f.FileName.EndsWith(".session") select f;
                    numberOfDeletedFiles = deletedFiles.Count();

                    recordingMetaDataFiles = files
                        .Where(f => f.Type == 1 && f.FileName.EndsWith(".session") && f.DeletedBy == String.Empty)
                        .Count();

                    deletedRecordingMetaDataFiles = files
                        .Where(f => f.Type == 1 && f.FileName.EndsWith(".session") && f.DeletedBy != String.Empty)
                        .Count();

                    temporaryAccountObjects = files
                        .Where(f => f.Type == 2 && f.SafeName.ToLower().EndsWith("_workspace") && f.DeletedBy == String.Empty)
                        .Count();

                    tempAdHocAccounts = files
                        .Where(f => f.Type == 2 && f.SafeName.ToLower() == "psmunmanagedsessionaccounts" && f.DeletedBy == String.Empty)
                        .Count();

                    temporaryDeletedAccountObjects = files.Where(f => f.Type == 2 && f.SafeName.ToLower().EndsWith("_workspace") && f.DeletedBy != String.Empty)
                        .Count();

                    effectiveNumberOfObjects = numberOfObjects - numberOfDeletedObjects;
                    effectiveNumberOfAccounts = numberOfAccounts - numberOfDeletedAccounts;
                    effectiveNumberOfFiles = numberOfFiles - numberOfDeletedFiles;

     
                    deletedObjectsGroupedByDeletionDate = new DataTable();
                    deletedObjectsGroupedByDeletionDate = (from d in deletedObjects
                                                               group d by new { month = d.DeletionDate.Month, year = d.DeletionDate.Year } into d
                                                               select new
                                                               {
                                                                   dt = String.Format("{0:00}" + @"/" + d.Key.year, d.Key.month),
                                                                   count = d.Count(),
                                                                   sorter = d.Key.year * 13 + d.Key.month,
                                                                   totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                               }).ToDataTable();
    

                    deletedObjectsGroupedByDeletionDate = addMissingMonths(minimumDate, maximumDate, deletedObjectsGroupedByDeletionDate);
                    deletedAccountsGroupedByDeletionDate = (from d in deletedAccounts
                                                            group d by new { month = d.DeletionDate.Month, year = d.DeletionDate.Year } into d
                                                            select new
                                                            {
                                                                dt = String.Format("{0:00}" + @"/" + d.Key.year, d.Key.month),
                                                                count = d.Count(),
                                                                sorter = d.Key.year * 13 + d.Key.month,
                                                                totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                            }).ToDataTable();
                    deletedAccountsGroupedByDeletionDate = addMissingMonths(minimumDate, maximumDate, deletedAccountsGroupedByDeletionDate);


                    deletedFilesGroupedByDeletionDate = new DataTable();
                    deletedFilesGroupedByDeletionDate = (from d in deletedFiles
                                                         group d by new { month = d.DeletionDate.Month, year = d.DeletionDate.Year } into d
                                                         select new
                                                         {
                                                             dt = String.Format("{0:00}" + @"/" + d.Key.year, d.Key.month),
                                                             count = d.Count(),
                                                             sorter = d.Key.year * 13 + d.Key.month,
                                                             totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                         }).ToDataTable();
                    deletedFilesGroupedByDeletionDate = addMissingMonths(minimumDate, maximumDate, deletedFilesGroupedByDeletionDate);

                    allDeletedObjectsGroupedByDeletionDate.Columns.Add("Date", typeof(string));
                    allDeletedObjectsGroupedByDeletionDate.Columns.Add("Objects", typeof(int));
                    allDeletedObjectsGroupedByDeletionDate.Columns.Add("Accounts", typeof(int));
                    allDeletedObjectsGroupedByDeletionDate.Columns.Add("Files", typeof(int));

                    for (int i = 0; i < deletedObjectsGroupedByDeletionDate.Rows.Count; i++)
                    {
                        allDeletedObjectsGroupedByDeletionDate.Rows.Add(deletedObjectsGroupedByDeletionDate.Rows[i][0], deletedObjectsGroupedByDeletionDate.Rows[i][1], deletedAccountsGroupedByDeletionDate.Rows[i][1], deletedFilesGroupedByDeletionDate.Rows[i][1]);
                    }

                    safesGroupByCreationDate = (from p in safes
                                                where p.CreationDate.Year != 1
                                                group p by new { month = p.CreationDate.Month, year = p.CreationDate.Year } into d
                                                select new
                                                {
                                                    dt = String.Format("{0:00}" + @"/" + d.Key.year, d.Key.month),
                                                    count = d.Count(),
                                                    sorter = d.Key.year * 13 + d.Key.month,
                                                    totalSize = 0
                                                }).ToDataTable();
                    safesGroupByCreationDate = addMissingMonths(minimumDate, maximumDate, safesGroupByCreationDate);


                    safesCountByPeriod.Columns.Add("dt", typeof(string));
                    safesCountByPeriod.Columns.Add("count", typeof(int));
                    for (int i = 0; i < safesGroupByCreationDate.Rows.Count; i++)
                    {
                        if (invisibleSafes > 0 && invisibleSafesAdded == false)
                        {
                            safesCountByPeriod.Rows.Add(safesGroupByCreationDate.Rows[i][0].ToString(),
                                Int32.Parse(safesGroupByCreationDate.Rows[i][1].ToString()) + safesCount + invisibleSafes);
                            safesCount += Int32.Parse(safesGroupByCreationDate.Rows[i][1].ToString()) + invisibleSafes;
                            invisibleSafesAdded = true;
                        }
                        else
                        {
                            safesCountByPeriod.Rows.Add(safesGroupByCreationDate.Rows[i][0].ToString(),
                                Int32.Parse(safesGroupByCreationDate.Rows[i][1].ToString()) + safesCount);
                            safesCount += Int32.Parse(safesGroupByCreationDate.Rows[i][1].ToString());
                        }
                    }

                    objectsInVault.Columns.Add("Object Type", typeof(string));
                    objectsInVault.Columns.Add("NumberOfObjects", typeof(int));
                    objectsInVault.Columns.Add("Share (%)", typeof(double));
                    objectsInVault.Rows.Add("Files", effectiveNumberOfFiles, Math.Round(effectiveNumberOfFiles * 100.00 / (effectiveNumberOfObjects - recordingMetaDataFiles - temporaryAccountObjects - tempAdHocAccounts), 2));
                    objectsInVault.Rows.Add("Accounts", effectiveNumberOfAccounts, Math.Round(effectiveNumberOfAccounts * 100.00 / (effectiveNumberOfObjects - recordingMetaDataFiles - temporaryAccountObjects - tempAdHocAccounts), 2));

                    queryString = "select Safename, (select Count(Filename) as Files from files where Safes.SafeID = Files.SafeID) as [Objects (including deleted objects)], (select Count(Filename) as [Files (including deleted files)] from files where Files.Type = 2 and Safes.SafeID = Files.SafeID) as [Accounts (including deleted accounts)] , (select Count(Filename) as Files from files where Files.Type = 1 and Safes.SafeID = Files.SafeID) as [Files (including deleted files)], (select Count(Filename) as Files from files where Files.DeletedBy = '' and Safes.SafeID = Files.SafeID) as [Objects (excluding deleted objects)], (select Count(Filename) from files where Files.Type = 2 and Files.DeletedBy = '' and Safes.SafeID = Files.SafeID) as [Accounts (excluding deleted accounts)], (select Count(Filename) as Files from files where Files.Type = 1 and Files.DeletedBy = '' and Safes.SafeID = Files.SafeID) as [Files (excluding deleted files)] from Safes order by 2 desc";
                    da.Fill(safesObjectFilesAccounts);

                    safesObjectFilesAccountsOrderedByAccounts = safesObjectFilesAccounts.Copy();
                    safesObjectFilesAccountsOrderedByAccounts.DefaultView.Sort = "Accounts (including deleted accounts) DESC";
                    safesObjectFilesAccountsOrderedByAccounts = safesObjectFilesAccountsOrderedByAccounts.DefaultView.ToTable();
                    safesObjectFilesAccountsOrderedByAccounts = safesObjectFilesAccountsOrderedByAccounts.AsEnumerable().Take(10).CopyToDataTable();

                    safesObjectFilesAccountsOrderedByFiles = safesObjectFilesAccounts.Copy();
                    safesObjectFilesAccountsOrderedByFiles.DefaultView.Sort = "Files (including deleted files) DESC";
                    safesObjectFilesAccountsOrderedByFiles = safesObjectFilesAccountsOrderedByFiles.DefaultView.ToTable();
                    safesObjectFilesAccountsOrderedByFiles = safesObjectFilesAccountsOrderedByFiles.AsEnumerable().Take(10).CopyToDataTable();

                    queryString = "select SafeID, Cast (ShareOptions as Text) as ShareOptions, SafeName, case when locationname = '\\' then 'Root' else substr(LocationName, 2, length(LocationName)) end as LocationName, Round(Cast (Size as Float) /1024.00,3) as [Used Size (MB)], Round(Cast (MaxSize as Float)) as [MaxSize (MB)], Round(Cast(Size as Float) /1024.00 * 100 / Cast(MaxSize as Float),3) as [UsedSize %], LogRetentionPeriod, ObjectsRetentionPeriod, RequestRetentionPeriod, Case NumberOfPasswordVersions When 0 Then -1 Else NumberOfPasswordVersions end as NumberOfPasswordVersions, QuotaOwner, CreationDate, CreatedBy, 'No' as [Access to fully impersonated users], 'No' as [Access to partially impersonated users], 'No' as [Enfore safe opening from PrivateArk client], 'No' as [Access with additional server authentication] from safes";
                    da.Fill(safesDataTable);


                    queryString = "select 'Days retention' as RetentionType, COUNT(case when NumberOfPasswordVersions < 1 then 1 else null end) as Safes, Round((COUNT(case when NumberOfPasswordVersions < 1 then 1 else null end)) * 100.00 / (select count(*) from safes),2) as [Share (%)] from safes union all select 'Versions retention', COUNT(case when NumberOfPasswordVersions > 0 then 1 else null end) as Safes, Round((COUNT(case when NumberOfPasswordVersions > 0 then 1 else null end)) * 100.00 / (select count(*) from safes),2) as [Share (%)] from safes order by Safes desc";
                    DBFunctions.closeDBConnection();

                    queryString = "select Case when NumberOfPasswordVersions > 1 then NumberOfPasswordVersions || ' versions' else NumberOfPasswordVersions || ' versions'  end  as RetentionSetting, COUNT(*) as Safes, Round(Count(*) * 100.00 / (select count(*) from safes),2) as [Share (%)] from Safes where  NumberOfPasswordVersions > 0 Group by NumberOfPasswordVersions union all select Case when ObjectsRetentionPeriod = 1 then ObjectsRetentionPeriod || ' day' else ObjectsRetentionPeriod || ' days' end as RetentionSetting, COUNT(*) as Safes, Round(Count(*) * 100.00 / (select count(*) from safes),2) as [Share (%)] from Safes where  NumberOfPasswordVersions < 1 Group by ObjectsRetentionPeriod order by safes desc";
                    da.Fill(mostUsedSafeRetentionSettings);


                    for (int i = 0; i < safesDataTable.Rows.Count; i++)
                    {
                        if (safesDataTable.Rows[i]["ShareOptions"] == DBNull.Value || safesDataTable.Rows[i]["ShareOptions"].ToString().Trim() == "" || !Int32.TryParse(safesDataTable.Rows[i]["ShareOptions"].ToString(), out int result))
                        {
                            safesDataTable.Rows[i]["ShareOptions"] = 0;
                        }

                        safesDataTable.Rows[i]["ShareOptions"] = Convert.ToString(Convert.ToUInt32(safesDataTable.Rows[i]["ShareOptions"].ToString()), 2).PadLeft(4, '0');

                        if (safesDataTable.Rows[i]["ShareOptions"].ToString()[safesDataTable.Rows[i]["ShareOptions"].ToString().Length - 1].ToString() == "1")
                        {
                            safesDataTable.Rows[i]["Access to fully impersonated users"] = "Yes";
                        }
                        if (safesDataTable.Rows[i]["ShareOptions"].ToString()[safesDataTable.Rows[i]["ShareOptions"].ToString().Length - 2].ToString() == "1")
                        {
                            safesDataTable.Rows[i]["Access to partially impersonated users"] = "Yes";
                        }
                        if (safesDataTable.Rows[i]["ShareOptions"].ToString()[safesDataTable.Rows[i]["ShareOptions"].ToString().Length - 3].ToString() == "1")
                        {
                            safesDataTable.Rows[i]["Access with additional server authentication"] = "Yes";
                        }
                        if (safesDataTable.Rows[i]["ShareOptions"].ToString()[safesDataTable.Rows[i]["ShareOptions"].ToString().Length - 4].ToString() == "1")
                        {
                            safesDataTable.Rows[i]["Enfore safe opening from PrivateArk client"] = "Yes";
                        }
                    }

                    safesDataTable.Columns.Remove("ShareOptions");


                    largestSafes = safes
                        .Select(i => new
                        {
                            i.SafeName,
                            Size = Math.Round(i.Size / 1024.00 / 1024.00, 2),
                            UsedSize = Math.Round(i.Size / 1024.00 * 100.00 / i.MaxSize, 2),
                            MaxSize = Math.Round(i.MaxSize / 1024.00, 2)
                        })
                        .OrderByDescending(i => i.Size)
                        .ToDataTable();


                    topSafeCreators = new DataTable();
                    topSafeCreators = (from p in safes
                                       group p by new { p.CreatedBy }
                            into d
                                       select new { UserName = d.Key.CreatedBy, CreatedSafes = d.Count() })
                        .OrderByDescending(x => x.CreatedSafes).ToDataTable();

                    foreach (DataRow row in topSafeCreators.Rows)
                    {
                        if (row["UserName"].ToString() == "")
                        {
                            row["UserName"] = "Master";
                        }
                    }

                    objectsGroupedByCreationDate = new DataTable();
                    objectsGroupedByCreationDate = (from p in files
                                                    group p by new { month = p.CreationDate.Month, year = p.CreationDate.Year }
                        into d
                                                    orderby d ascending
                                                    select new
                                                    {
                                                        dt = String.Format("{0:00}" + @"/" + d.Key.year.ToString(), d.Key.month),
                                                        count = d.Count(),
                                                        sorter = d.Key.year * 13 + d.Key.month,
                                                        totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                    }).ToDataTable();

                    accountsGroupedByCreationDate = new DataTable();
                    accountsGroupedByCreationDate = (from p in files
                                                     where p.Type == 2 && !p.SafeName.ToLower().Contains("_workspace") && p.SafeName.ToLower() != "psmunmanagedsessionaccounts"
                                                     group p by new { month = p.CreationDate.Month, year = p.CreationDate.Year }
                        into d
                                                     orderby d ascending
                                                     select new
                                                     {
                                                         dt = String.Format("{0:00}" + @"/" + d.Key.year.ToString(), d.Key.month),
                                                         count = d.Count(),
                                                         sorter = d.Key.year * 13 + d.Key.month,
                                                         totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                     }).ToDataTable();

                    filesGroupedByCreationDate = new DataTable();
                    filesGroupedByCreationDate = (from p in files
                                                  where p.Type == 1 && !p.FileName.EndsWith(".session")
                                                  group p by new { month = p.CreationDate.Month, year = p.CreationDate.Year }
                        into d
                                                  orderby d ascending
                                                  select new
                                                  {
                                                      dt = String.Format("{0:00}" + @"/" + d.Key.year.ToString(), d.Key.month),
                                                      count = d.Count(),
                                                      sorter = d.Key.year * 13 + d.Key.month,
                                                      totalSize = d.Sum(x => x.Size / 1073741824.00)
                                                  }).ToDataTable();

                    objectsGroupedByCreationDate = addMissingMonths(minimumDate, maximumDate, objectsGroupedByCreationDate);
                    accountsGroupedByCreationDate = addMissingMonths(minimumDate, maximumDate, accountsGroupedByCreationDate);
                    filesGroupedByCreationDate = addMissingMonths(minimumDate, maximumDate, filesGroupedByCreationDate);


                    allObjectsGroupedByCreationDate.Columns.Add("Date", typeof(string));
                    allObjectsGroupedByCreationDate.Columns.Add("Objects", typeof(int));
                    allObjectsGroupedByCreationDate.Columns.Add("Accounts", typeof(int));
                    allObjectsGroupedByCreationDate.Columns.Add("Files", typeof(int));

                    for (int i = 0; i < objectsGroupedByCreationDate.Rows.Count; i++)
                    {
                        allObjectsGroupedByCreationDate.Rows.Add(objectsGroupedByCreationDate.Rows[i][0], objectsGroupedByCreationDate.Rows[i][1], accountsGroupedByCreationDate.Rows[i][1], filesGroupedByCreationDate.Rows[i][1]);
                    }

                    totalFilesGrowth = new DataTable();
                    totalFilesGrowth.Columns.Add("Date", typeof(string));
                    totalFilesGrowth.Columns.Add("TotalSize (GB)", typeof(double));
                    totalFilesGrowth.Columns.Add("DataGrowth (GB)", typeof(double));

                    numberOfObjectsDifference = new DataTable();
                    numberOfObjectsDifference.Columns.Add("Date", typeof(string));
                    numberOfObjectsDifference.Columns.Add("Objects", typeof(int));
                    numberOfObjectsDifference.Columns.Add("Accounts", typeof(int));
                    numberOfObjectsDifference.Columns.Add("Files", typeof(int));

                    objectsCountByPeriod = new DataTable();
                    objectsCountByPeriod.Columns.Add("Date", typeof(string));
                    objectsCountByPeriod.Columns.Add("Objects", typeof(int));
                    objectsCountByPeriod.Columns.Add("Accounts", typeof(int));
                    objectsCountByPeriod.Columns.Add("Files", typeof(int));

                    int objectsCount = 0;
                    int filesCount = 0;
                    int accountsCount = 0;
                    double totalSize = 0;

                    // Determining objects development...

                    for (int i = 0; i < deletedObjectsGroupedByDeletionDate.Rows.Count; i++)
                    {
                        for (int j = 0; j < objectsGroupedByCreationDate.Rows.Count; j++)
                        {

                            if (deletedObjectsGroupedByDeletionDate.Rows[i][0].ToString() == objectsGroupedByCreationDate.Rows[j][0].ToString())
                            {
                                int objectsDifference = Int32.Parse(objectsGroupedByCreationDate.Rows[j][1].ToString()) -
                                                        Int32.Parse(deletedObjectsGroupedByDeletionDate.Rows[i][1]
                                                            .ToString());
                                int accountsDifference = Int32.Parse(accountsGroupedByCreationDate.Rows[j][1].ToString()) -
                                                         Int32.Parse(deletedAccountsGroupedByDeletionDate.Rows[i][1]
                                                             .ToString());

                                int filesDifference = Int32.Parse(filesGroupedByCreationDate.Rows[j][1].ToString()) -
                                                      Int32.Parse(deletedFilesGroupedByDeletionDate.Rows[i][1].ToString());


                                double growth = Double.Parse(objectsGroupedByCreationDate.Rows[j][3].ToString()) - Double.Parse(deletedObjectsGroupedByDeletionDate.Rows[i][3].ToString());
                                totalSize += growth;

                                if (totalSize < 1)
                                {
                                    totalFilesGrowth.Rows.Add(deletedObjectsGroupedByDeletionDate.Rows[i][0], Math.Round(totalSize, 3), Math.Round(growth, 3));
                                }
                                else
                                {
                                    totalFilesGrowth.Rows.Add(deletedObjectsGroupedByDeletionDate.Rows[i][0], Math.Round(totalSize, 2), Math.Round(growth, 2));
                                }

                                numberOfObjectsDifference.Rows.Add(deletedObjectsGroupedByDeletionDate.Rows[i][0], objectsDifference, accountsDifference, filesDifference);

                                objectsCount += objectsDifference;
                                accountsCount += accountsDifference;
                                filesCount += filesDifference;

                                objectsCountByPeriod.Rows.Add(deletedFilesGroupedByDeletionDate.Rows[i][0], objectsCount, accountsCount, filesCount);
                            }
                        }
                    }


                    totalSizeOfAllSafes =
                        Math.Round(totalSize, 2);
                    averageSafeSize =
                        Math.Round(totalSize / numberOfSafes, 2);
                    totalMaxSize = Math.Round(safes.Select(x => x.MaxSize).Sum() / 1024.00, 2);
                    averageSafeMaxSize = Math.Round(safes.Average(x => x.MaxSize) / 1024.00, 2);

                    safeRetentionSettings = new DataTable();
                    queryString = "Select SafeName, ObjectsRetentionPeriod, LogRetentionPeriod, RequestRetentionPeriod, case NumberOfPasswordVersions when 0 then -1 else NumberOfPasswordVersions End as NumberOfPasswordVersions from Safes;";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(safeRetentionSettings);
                    DBFunctions.closeDBConnection();


                    safesCreationDataTable = (from table1 in safesCountByPeriod.AsEnumerable()
                                              join table2 in safesGroupByCreationDate.AsEnumerable() on (string)table1[0] equals (string)table2[0]
                                              select new
                                              {
                                                  Date = (string)table1[0],
                                                  CreatedSafes = (int)table2[1],
                                                  TotalNumberOfSafes = (int)table1[1]
                                              }).ToDataTable();




                    if (users.Count() > 0 && groups.Count() > 0 && groupmembers.Count() > 0)
                    {

                        Console.WriteLine(DateTime.Now + " Determining users information...");

                        tempDataTable = new DataTable();

                        bool licenseCapacityReportExists = false;
                        if (hasLicenseXML && DBFunctions.checkIfTableExists("LicenseCapacity"))
                        {
                            licenseCapacityReportExists = true;


                            if (File.Exists(@"data/Config files/" + settings.LicenseXmlFile) && settings.DeleteLicenseXmlCopy)
                            {
                                secureDelete(@"data/Config files/" + settings.LicenseXmlFile);
                            }
                        }


                        groupMembersDataTable.Columns.Add("GroupID", typeof(int));
                        groupMembersDataTable.Columns.Add("UserID", typeof(int));
                        groupMembersDataTable.Columns.Add("MemberIsGroup", typeof(string));
                        groupMembersDataTable.Columns.Add("GroupName", typeof(string));
                        groupMembersDataTable.Columns.Add("GroupOrigin", typeof(string));
                        groupMembersDataTable.Columns.Add("Member", typeof(string));
                        groupMembersDataTable.Columns.Add("MemberType", typeof(string));
                        groupMembersDataTable.Columns.Add("MemberOrigin", typeof(string));

                        queryString = "Create table if not exists Memberships as select GroupMembers.GroupID, GroupMembers.UserID, GroupMembers.MemberIsGroup, g.GroupName Collate Nocase, g.GroupOrigin, u.Member Collate Nocase, u.MemberType, u.MemberOrigin  from GroupMembers left join (select GroupID, GroupName, Case [Internal/External] When 1 Then 'Internal' else 'External' End as GroupOrigin from Groups) g on GroupMembers.GroupID = g.GroupID left join (select UserID, UserName as Member, 'User' as MemberType, Case [Internal/External] When 1 Then 'Internal' else 'External' End as MemberOrigin from Users) u on GroupMembers.UserID = u.UserID where GroupMembers.MemberIsGroup = 'NO' union select GroupMembers.GroupID, GroupMembers.UserID, GroupMembers.MemberIsGroup, g.GroupName, g.GroupOrigin, u.Member, u.MemberType, u.MemberOrigin from GroupMembers left join (select GroupID, GroupName, Case [Internal/External] When 1 Then 'Internal' else 'External' End as GroupOrigin from Groups) g on GroupMembers.GroupID = g.GroupID left join (select GroupID as GroupID, GroupName as Member, 'Group' as MemberType, Case [Internal/External] When 1 Then 'Internal' else 'External' End as MemberOrigin from Groups) u on GroupMembers.UserID = u.GroupID where GroupMembers.MemberIsGroup = 'YES' order by u.MemberType;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();

                        tempDataTable = new DataTable();
                        queryString = "Create INDEX if not exists i_Memberships_1 ON Memberships(Member, MemberType);";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();


                        tempDataTable = new DataTable();
                        queryString = "Create INDEX if not exists i_Memberships_2 ON Memberships(GroupName, MemberType);";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();


                        tempDataTable = new DataTable();
                        queryString = "Create INDEX if not exists i_Memberships_3 ON Memberships(Member);";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();

                        queryString = "Select * from Memberships";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(groupMembersDataTable);
                        DBFunctions.closeDBConnection();

                        queryString = "select GroupID, GroupName, 0 as TotalUserMembers, 0 as TotalGroupMembers, 0 as GroupMemberships, 'None' as Information, LocationName, Case [Internal/External] When 1 Then 'Internal' else 'External' End as 'Origin', LDAPDirectory, LDAPFullDN, MapName, Description from groups";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(groupsDataTable);
                        DBFunctions.closeDBConnection();

                        queryString = "select UserName, UserID, 0 as GroupMemberships, 'Unknown' as UserType, Case [Internal/External] When 1 Then 'Internal' else 'External' End as 'Origin', CASE UserName WHEN 'Master' THEN 'Specific' WHEN 'Batch' THEN 'Password' ELSE (CASE AuthenticationMethods WHEN 0 THEN 'LDAP' WHEN 1 THEN 'Password' WHEN 2 THEN 'PKI' WHEN 3 THEN 'SECUREID' WHEN 8 THEN 'NTAuth' WHEN 16 THEN 'RADIUS' ELSE 'Unknown' END) END as AuthenticationMethod, LocationName, MapName, LDAPDirectory, FirstName, LastName, Case When Disabled = 'NO' Then 'No' Else 'Yes' End as Disabled, Case When PasswordNeverExpires = 'NO' then 'No' Else 'Yes' End as PasswordNeverExpires, Case Authorizations When 1023 Then 'Yes' else 'No' End as AllAuthorizations, 'No' as [Add/UpdateUsers], 'No' as AddSafes, 'No' as ManageDirectoryMappings, 'No' as AddNetworkAreas, 'No' as ManageServerFileCategories, 'No' as AuditUsers, 'No' as BackupAllSafes, 'No' as RestoreAllSafes, 'No' as ResetUserPasswords, 'No' as ActivateUsers, Authorizations as [Authorizations], UserTypeID, CreationDate, LastLogonDate, PrevLogonDate, LDAPFullDN, DistinguishedName, RestrictedInterfaces, BusinessEmail, FromHour, ToHour, 'No' as PartialImpersonation, 'No' as FullImpersonation, 'No' as ImpersonationWithServerAuthentication, GatewayAccountAuthorizations, LogRetentionPeriod, ApplicationMetaData from users where UserName not in (select distinct(mapname) from users where mapname != '' union select distinct(mapname) from groups where mapname != '')";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(usersDataTable);
                        DBFunctions.closeDBConnection();
                        string location = string.Empty;
                        usersDataTable.Columns.Add("LogonDaysAgo", typeof(int)).SetOrdinal(4);
                        usersDataTable.Columns.Add("PrevLogonDaysAgo", typeof(int)).SetOrdinal(28);

                        epvUsersByAuthenticationMethod = (from u in usersDataTable.AsEnumerable()
                                                          where u.Field<Int64>("UserTypeID") == 34
                                                          group u by u.Field<string>("AuthenticationMethod") into m
                                                          orderby m.Count() descending
                                                          select new
                                                          {
                                                              AuthenticationMethod = m.Key,
                                                              EPVUsers = m.Count()
                                                          }).ToDataTable();

                        queryString = "select LDAPDirectory, MapName, count(MapName) as Users from users where MapName != '' group by MapName order by Users desc";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(mappingsDataTable);
                        DBFunctions.closeDBConnection();

                        ldapDirectoriesDataTable = (from d in usersDataTable.AsEnumerable()
                                                    where d.Field<string>("LDAPDirectory") != ""
                                                    group d by d.Field<string>("LDAPDirectory") into m
                                                    orderby m.Count() descending
                                                    select new
                                                    {
                                                        LDAPDirectory = m.Key,
                                                        Users = m.Count()
                                                    }).ToDataTable();

                        usersByLastLogon.Columns.Add("LastLogon", typeof(string));
                        usersByLastLogon.Columns.Add("Users", typeof(int));
                        usersByLastLogon.Columns.Add("Users (%)", typeof(double));
                        usersByLastLogon.Rows.Add("Last 7 days", 0);
                        usersByLastLogon.Rows.Add("More than 7 days ago", 0);
                        usersByLastLogon.Rows.Add("More than 30 days ago", 0);
                        usersByLastLogon.Rows.Add("More than 90 days ago", 0);
                        usersByLastLogon.Rows.Add("More than 180 days ago", 0);
                        usersByLastLogon.Rows.Add("More than 365 days ago", 0);

                        usersByLastLogon2.Columns.Add("LastLogon", typeof(string));
                        usersByLastLogon2.Columns.Add("Users", typeof(int));
                        usersByLastLogon2.Columns.Add("Users (%)", typeof(double));
                        usersByLastLogon2.Rows.Add("Between 0 and 7 days ago", 0);
                        usersByLastLogon2.Rows.Add("Between 8 and 30 days ago", 0);
                        usersByLastLogon2.Rows.Add("Between 31 and 90 days ago", 0);
                        usersByLastLogon2.Rows.Add("Between 91 and 180 days ago", 0);
                        usersByLastLogon2.Rows.Add("Between 181 and 365 days ago", 0);
                        usersByLastLogon2.Rows.Add("More than 365 days ago", 0);


                        long authenticationMethod = 0;
                        long authorizations = 0;
                        long gatewayAuthorizations = 0;
                        int lastLogonDaysAgo = 0;
                        string authorizationsBinaryString = String.Empty;
                        string gatewayAuthorizationsBinaryString = String.Empty;
                        string authenticationMethodString = String.Empty;

                        queryString = "Select Count([Member]) from Memberships where [Member] = $userName and [MemberType] = 'User'";
                        using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                        {
                            con.Open();
                            using (SQLiteCommand cmd = con.CreateCommand())
                            {
                                cmd.CommandText = queryString;
                                for (int i = 0; i < usersDataTable.Rows.Count; i++)
                                {

                                    cmd.Parameters.AddWithValue("$userName", usersDataTable.Rows[i]["UserName"]);
                                    usersDataTable.Rows[i]["GroupMemberships"] = cmd.ExecuteScalar();

                                    if ((string)usersDataTable.Rows[i]["UserName"] == "Master")
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "Builtin";
                                        usersDataTable.Rows[i]["Origin"] = "Internal";
                                        usersDataTable.Rows[i]["PasswordNeverExpires"] = "Yes";
                                        usersDataTable.Rows[i]["Authorizations"] = 1023;
                                        usersDataTable.Rows[i]["AllAuthorizations"] = "Yes";
                                        usersDataTable.Rows[i]["CreationDate"] = (DateTime)usersDataTable.Rows[i + 1]["CreationDate"];
                                    }

                                    authorizations = (Int64)usersDataTable.Rows[i]["Authorizations"];
                                    authorizationsBinaryString = Convert.ToString(authorizations, 2);


                                    gatewayAuthorizations = (Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"];
                                    gatewayAuthorizationsBinaryString = Convert.ToString(gatewayAuthorizations, 2);


                                    if (authorizations % 2 == 1)
                                    {
                                        usersDataTable.Rows[i]["Add/UpdateUsers"] = "Yes";
                                    }
                                    if (authorizations > 1 && authorizationsBinaryString[authorizationsBinaryString.Length - 2] == '1')
                                    {
                                        usersDataTable.Rows[i]["AddSafes"] = "Yes";
                                    }
                                    if (authorizations > 3 && authorizationsBinaryString[authorizationsBinaryString.Length - 3] == '1')
                                    {
                                        usersDataTable.Rows[i]["ManageDirectoryMappings"] = "Yes";
                                    }
                                    if (authorizations > 7 && authorizationsBinaryString[authorizationsBinaryString.Length - 4] == '1')
                                    {
                                        usersDataTable.Rows[i]["AddNetworkAreas"] = "Yes";
                                    }
                                    if (authorizations > 15 && authorizationsBinaryString[authorizationsBinaryString.Length - 5] == '1')
                                    {
                                        usersDataTable.Rows[i]["ManageServerFileCategories"] = "Yes";
                                    }
                                    if (authorizations > 31 && authorizationsBinaryString[authorizationsBinaryString.Length - 6] == '1')
                                    {
                                        usersDataTable.Rows[i]["AuditUsers"] = "Yes";
                                    }
                                    if (authorizations > 63 && authorizationsBinaryString[authorizationsBinaryString.Length - 7] == '1')
                                    {
                                        usersDataTable.Rows[i]["BackupAllSafes"] = "Yes";
                                    }
                                    if (authorizations > 127 && authorizationsBinaryString[authorizationsBinaryString.Length - 8] == '1')
                                    {
                                        usersDataTable.Rows[i]["RestoreAllSafes"] = "Yes";
                                    }
                                    if (authorizations > 255 && authorizationsBinaryString[authorizationsBinaryString.Length - 9] == '1')
                                    {
                                        usersDataTable.Rows[i]["ResetUserPasswords"] = "Yes";
                                    }
                                    if (authorizations > 511 && authorizationsBinaryString[authorizationsBinaryString.Length - 10] == '1')
                                    {
                                        usersDataTable.Rows[i]["ActivateUsers"] = "Yes";
                                    }

                                    if (gatewayAuthorizations % 2 == 1)
                                    {
                                        usersDataTable.Rows[i]["FullImpersonation"] = "Yes";
                                    }
                                    if (gatewayAuthorizations > 1 && gatewayAuthorizationsBinaryString[gatewayAuthorizationsBinaryString.Length - 2] == '1')
                                    {
                                        usersDataTable.Rows[i]["PartialImpersonation"] = "Yes";
                                    }
                                    if (gatewayAuthorizations > 3 && gatewayAuthorizationsBinaryString[gatewayAuthorizationsBinaryString.Length - 3] == '1')
                                    {
                                        usersDataTable.Rows[i]["ImpersonationWithServerAuthentication"] = "Yes";
                                    }

                                    location = (string)usersDataTable.Rows[i]["LocationName"];

                                    if (location != "\\")
                                    {
                                        usersDataTable.Rows[i]["LocationName"] = location.Substring(1);
                                    }
                                    else
                                    {
                                        usersDataTable.Rows[i]["LocationName"] = "Root";
                                    }

                                    if (usersDataTable.Rows[i]["LastLogonDate"].ToString() != "" && Convert.ToDateTime(usersDataTable.Rows[i]["LastLogonDate"]).Year != 1)
                                    {
                                        lastLogonDaysAgo = (int)Math.Floor((reportDate - Convert.ToDateTime(usersDataTable.Rows[i]["LastLogonDate"].ToString()).Date).TotalDays);
                                        usersDataTable.Rows[i]["LogonDaysAgo"] = lastLogonDaysAgo;

                                        if (lastLogonDaysAgo < 8)
                                        {
                                            usersByLastLogon.Rows[0][1] = (int)usersByLastLogon.Rows[0][1] + 1;
                                            usersByLastLogon2.Rows[0][1] = (int)usersByLastLogon2.Rows[0][1] + 1;

                                        }
                                        if (lastLogonDaysAgo > 7)
                                        {
                                            usersByLastLogon.Rows[1][1] = (int)usersByLastLogon.Rows[1][1] + 1;
                                            if (lastLogonDaysAgo < 31)
                                            {
                                                usersByLastLogon2.Rows[1][1] = (int)usersByLastLogon2.Rows[1][1] + 1;
                                            }
                                        }
                                        if (lastLogonDaysAgo > 30)
                                        {
                                            usersByLastLogon.Rows[2][1] = (int)usersByLastLogon.Rows[2][1] + 1;
                                            if (lastLogonDaysAgo < 91)
                                            {
                                                usersByLastLogon2.Rows[2][1] = (int)usersByLastLogon2.Rows[2][1] + 1;
                                            }
                                        }
                                        if (lastLogonDaysAgo > 90)
                                        {
                                            usersByLastLogon.Rows[3][1] = (int)usersByLastLogon.Rows[3][1] + 1;
                                            if (lastLogonDaysAgo < 181)
                                            {
                                                usersByLastLogon2.Rows[3][1] = (int)usersByLastLogon2.Rows[3][1] + 1;
                                            }
                                        }
                                        if (lastLogonDaysAgo > 180)
                                        {
                                            usersByLastLogon.Rows[4][1] = (int)usersByLastLogon.Rows[4][1] + 1;
                                            if (lastLogonDaysAgo < 366)
                                            {
                                                usersByLastLogon2.Rows[4][1] = (int)usersByLastLogon2.Rows[4][1] + 1;
                                            }
                                        }
                                        if (lastLogonDaysAgo > 365)
                                        {
                                            usersByLastLogon.Rows[5][1] = (int)usersByLastLogon.Rows[5][1] + 1;
                                            usersByLastLogon2.Rows[5][1] = (int)usersByLastLogon2.Rows[5][1] + 1;
                                        }

                                    }

                                    if (usersDataTable.Rows[i]["PrevLogonDate"].ToString() != "" && Convert.ToDateTime(usersDataTable.Rows[i]["PrevLogonDate"]).Year != 1)
                                    {
                                        usersDataTable.Rows[i]["PrevLogonDaysAgo"] = Math.Floor((reportDate - Convert.ToDateTime(usersDataTable.Rows[i]["PrevLogonDate"].ToString()).Date).TotalDays);
                                    }

                                    if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 31)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "CPM";
                                        if ((string)usersDataTable.Rows[i]["Disabled"] == "No")
                                        {
                                            CPMs.Add((string)usersDataTable.Rows[i]["UserName"]);
                                        }
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 34)
                                    {
                                        if (!mappingsDataTable.AsEnumerable().Any(row => row.Field<string>("MapName") == (string)usersDataTable.Rows[i]["UserName"]))
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "EPV";
                                        }
                                        else
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "Builtin";
                                        }
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 11)
                                    {
                                        if ((string)usersDataTable.Rows[i]["UserName"] == "NotificationEngine")
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "Builtin";
                                        }
                                        else
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "ENE";
                                        }
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 74)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PTA";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 70 && (Int64)usersDataTable.Rows[i]["Authorizations"] == 34)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PSM for SSH APP";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 70 && ((Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"] > 4 || (Int64)usersDataTable.Rows[i]["Authorizations"] == 32))
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PSM for SSH GW";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 72 && (Int64)usersDataTable.Rows[i]["Authorizations"] > 31)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PSM for SSH ADB";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 32 && (Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"] > 4)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PVWA GW";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 32 && (Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"] == 0)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PVWA APP";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 36 && ((Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"] == 0 || (Int64)usersDataTable.Rows[i]["Authorizations"] == 34))
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PSM APP";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 36 && ((Int64)usersDataTable.Rows[i]["GatewayAccountAuthorizations"] == 7 || (Int64)usersDataTable.Rows[i]["Authorizations"] == 0))
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "PSM GW";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 501 || (Int64)usersDataTable.Rows[i]["UserTypeID"] == 502)
                                    {
                                        if ((Int64)usersDataTable.Rows[i]["Authorizations"] == 192)
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "DR";
                                        }
                                        if ((Int64)usersDataTable.Rows[i]["Authorizations"] == 64)
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "Backup";
                                        }
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 504 && (Int64)usersDataTable.Rows[i]["Authorizations"] == 192)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "DR";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 33)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "AAM Provider";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 35)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "AAM Application";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 37)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "OPM Agent";
                                    }
                                    else if ((Int64)usersDataTable.Rows[i]["UserTypeID"] == 10)
                                    {
                                        usersDataTable.Rows[i]["UserType"] = "Builtin";
                                        if ((string)usersDataTable.Rows[i]["UserName"] == "DR")
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "DR";
                                        }
                                        if ((string)usersDataTable.Rows[i]["UserName"] == "Backup")
                                        {
                                            usersDataTable.Rows[i]["UserType"] = "Backup";
                                        }
                                    }
                                    if ((string)usersDataTable.Rows[i]["UserType"] == "Unknown" && usersDataTable.Rows[i]["UserTypeID"] != null && hasLicenseXML && licenseCapacityReportExists)
                                    {                                 
                                            command = new SQLiteCommand("select [Licensed object] from LicenseCapacity where UserTypeID = " + usersDataTable.Rows[i]["UserTypeID"].ToString().Trim(), DBFunctions.connectToDB());
                                            string type = string.Empty;
                                            type = command.ExecuteScalar().ToString();
                                            if (type != null && type != string.Empty)
                                            {
                                                usersDataTable.Rows[i]["UserType"] = type;
                                            }
                                            DBFunctions.closeDBConnection();                                     
                                    }

                                }

                            }
                            con.Close();
                        }

                        if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("EPV")).ToList().Count == 0)
                        {
                            Console.WriteLine("Warning: No EPV users could be determined. The reports generation is likely to fail.");
                        }

                        tempDataTable = new DataTable();
                        tempDataTable = usersDataTable.Copy();
                        tempDataTable.TableName = "TempUsers";
                        DBFunctions.storeDataTableInSqliteDatabase(tempDataTable);

                        command = new SQLiteCommand("select LogRetentionPeriod || ' days' as AuditLogRetentionPeriod, Count(*) as Users from tempUsers group By LogRetentionPeriod order by LogRetentionPeriod desc", DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(usersByLogRetentionPeriod);
                        DBFunctions.closeDBConnection();


                        if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType") == "CPM").ToList().Count == 0)
                        {
                            Console.WriteLine(Environment.NewLine + "Warning: No CPM users could be determined from the EVD userlist export. This can lead to issues or incomplete reports information in this tool. Please ignore this message if you do not have CPMs installed in your environment." + Environment.NewLine);
                        }

                        CPMs.Sort();

                        userAuthorizationsDataTable.Columns.Add("VaultAuthorization", typeof(string));
                        userAuthorizationsDataTable.Columns.Add("Users", typeof(int));
                        userAuthorizationsDataTable.Rows.Add("Add Safes", usersDataTable.AsEnumerable().Count(row => row.Field<string>("AddSafes") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Audit Users", usersDataTable.AsEnumerable().Count(row => row.Field<string>("AuditUsers") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Add/Update Users", usersDataTable.AsEnumerable().Count(row => row.Field<string>("Add/UpdateUsers") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Reset User Passwords", usersDataTable.AsEnumerable().Count(row => row.Field<string>("ResetUserPasswords") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Activate Users", usersDataTable.AsEnumerable().Count(row => row.Field<string>("ActivateUsers") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Add Network Areas", usersDataTable.AsEnumerable().Count(row => row.Field<string>("AddNetworkAreas") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Manage Directory Mappings", usersDataTable.AsEnumerable().Count(row => row.Field<string>("ManageDirectoryMappings") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Manage Server File Categories", usersDataTable.AsEnumerable().Count(row => row.Field<string>("ManageServerFileCategories") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Backup All Safes", usersDataTable.AsEnumerable().Count(row => row.Field<string>("BackupAllSafes") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("Restore All Safes", usersDataTable.AsEnumerable().Count(row => row.Field<string>("RestoreAllSafes") == "Yes"));
                        userAuthorizationsDataTable.Rows.Add("All Authorizations", usersDataTable.AsEnumerable().Count(row => row.Field<Int64>("Authorizations") == 1023));
                        userAuthorizationsDataTable.DefaultView.Sort = "Users DESC";
                        userAuthorizationsDataTable = userAuthorizationsDataTable.DefaultView.ToTable();

                        totalNumberOfUsers = users.Count();
                        EPVUsers = usersDataTable.AsEnumerable().Count(row => row.Field<string>("UserType") == "EPV");
                        disabledUsers = usersDataTable.AsEnumerable().Count(row => row.Field<string>("Disabled") == "Yes");
                        enabledUsers = usersDataTable.AsEnumerable().Count(row => row.Field<string>("Disabled") == "No");
                        internalUsers = usersDataTable.AsEnumerable().Count(row => row.Field<string>("Origin") == "Internal");
                        ldapUsers = usersDataTable.AsEnumerable().Count(row => row.Field<string>("Origin") == "External");

                        usersByOrigin.Columns.Add("Origin", typeof(string));
                        usersByOrigin.Columns.Add("Users", typeof(int));
                        usersByOrigin.Columns.Add("Share (%)", typeof(double));
                        usersByOrigin.Rows.Add("Internal", internalUsers, Math.Round(internalUsers * 100.00 / totalNumberOfUsers, 2));
                        usersByOrigin.Rows.Add("External", ldapUsers, Math.Round(ldapUsers * 100.00 / totalNumberOfUsers, 2));


                        usersByOrigin.DefaultView.Sort = "Users DESC";
                        usersByOrigin = usersByOrigin.DefaultView.ToTable();


                        for (int i = 0; i < usersByLastLogon.Rows.Count; i++)
                        {
                            usersByLastLogon.Rows[i][2] = Math.Round((int)usersByLastLogon.Rows[i][1] * 100.00 / totalNumberOfUsers, 2);
                        }
                        for (int i = 0; i < usersByLastLogon2.Rows.Count; i++)
                        {
                            usersByLastLogon2.Rows[i][2] = Math.Round((int)usersByLastLogon2.Rows[i][1] * 100.00 / totalNumberOfUsers, 2);
                        }

                        usersDevelopmentDataTable = (from d in users
                                                     group d by new { month = d.CreationDate.Month, year = d.CreationDate.Year } into d
                                                     select new
                                                     {
                                                         dt = String.Format("{0:00}" + @"/" + d.Key.year, d.Key.month),
                                                         count = d.Count(),
                                                         sorter = d.Key.year * 13 + d.Key.month,
                                                         sum = 0
                                                     }).ToDataTable();


                        usersDevelopmentDataTable = addMissingMonths(minimumDate, reportDate, usersDevelopmentDataTable);
                        usersDevelopmentDataTable.Columns["dt"].ColumnName = "Date";
                        usersDevelopmentDataTable.Columns["count"].ColumnName = "CreatedUsers";
                        usersDevelopmentDataTable.Columns.Remove("sorter");
                        usersDevelopmentDataTable.Columns.Remove("sum");
                        usersDevelopmentDataTable.Columns.Add("TotalUsers", typeof(int));


                        int usersCount = 0;
                        for (int i = 0; i < usersDevelopmentDataTable.Rows.Count; i++)
                        {
                            usersDevelopmentDataTable.Rows[i][2] = 0;
                            usersCount += Int32.Parse(usersDevelopmentDataTable.Rows[i][1].ToString());
                            usersDevelopmentDataTable.Rows[i][2] = usersCount;
                        }

                        usersByType = (from p in usersDataTable.AsEnumerable()
                                       group p by p.Field<string>("UserType") into d
                                       orderby d.Count() descending
                                       select new
                                       {
                                           UserType = d.Key,
                                           Users = d.Count()
                                       }).ToDataTable();

                        usersByLDAP = (from p in usersDataTable.AsEnumerable()
                                       where p.Field<string>("LDAPDirectory") != ""
                                       group p by p.Field<string>("LDAPDirectory") into d
                                       orderby d.Count() descending
                                       select new
                                       {
                                           LDAPDirectory = d.Key,
                                           Users = d.Count()
                                       }).ToDataTable();

                        usersByType.Columns.Add("Enabled", typeof(int));
                        usersByType.Columns.Add("Enabled (%)", typeof(double));
                        usersByType.Columns.Add("Disabled", typeof(int));
                        usersByType.Columns.Add("Disabled (%)", typeof(double));

                        for (int i = 0; i < usersByType.Rows.Count; i++)
                        {
                            usersByType.Rows[i]["Enabled"] = usersDataTable.AsEnumerable().Count(row => row.Field<string>("UserType") == (string)usersByType.Rows[i]["UserType"] && row.Field<string>("Disabled") == "No");
                            usersByType.Rows[i]["Enabled (%)"] = Math.Round((int)usersByType.Rows[i]["Enabled"] * 100.00 / (int)usersByType.Rows[i]["Users"], 2);
                            usersByType.Rows[i]["Disabled"] = usersDataTable.AsEnumerable().Count(row => row.Field<string>("UserType") == (string)usersByType.Rows[i]["UserType"] && row.Field<string>("Disabled") == "Yes");
                            usersByType.Rows[i]["Disabled (%)"] = Math.Round((int)usersByType.Rows[i]["Disabled"] * 100.00 / (int)usersByType.Rows[i]["Users"], 2);
                        }

                        usersDevelopmentByUserTypeDataTable = usersDevelopmentDataTable.Copy();
                        usersDevelopmentByUserTypeDataTable.Columns.Remove("CreatedUsers");
                        usersDevelopmentByMapping = usersDevelopmentByUserTypeDataTable.Copy();
                        usersDevelopmentByMapping.Columns.Remove("TotalUsers");
                        usersDevelopmentByLDAP = usersDevelopmentByMapping.Copy();

                        for (int i = 0; i < usersByType.Rows.Count; i++)
                        {
                            usersDevelopmentByUserTypeDataTable.Columns.Add((string)usersByType.Rows[i][0], typeof(int));
                        }

                        for (int i = 0; i < usersDevelopmentByUserTypeDataTable.Rows.Count; i++)
                        {
                            for (int j = 2; j < usersDevelopmentByUserTypeDataTable.Columns.Count; j++)
                            {
                                usersDevelopmentByUserTypeDataTable.Rows[i][j] = 0;
                            }
                        }

                        DataSet usersDevelopmentByUserType = new DataSet();

                        for (int i = 0; i < usersByType.Rows.Count; i++)
                        {
                            DataTable tmpUsers = new DataTable();
                            tmpUsers = (from u in usersDataTable.AsEnumerable()
                                        where u.Field<string>("UserType") == usersByType.Rows[i][0].ToString()
                                        group u by String.Format("{0:00}" + @"/", u.Field<DateTime>("CreationDate").ToString().Split('-')[1]) + u.Field<DateTime>("CreationDate").ToString().Split('-')[0] into d
                                        select new
                                        {
                                            Date = d.Key,
                                            count = d.Count()
                                        }).ToDataTable();
                            usersDevelopmentByUserType.Tables.Add(tmpUsers);
                            usersDevelopmentByUserType.Tables[i].TableName = usersByType.Rows[i][0].ToString().Trim() + i.ToString();
                        }

                        foreach (DataTable table in usersDevelopmentByUserType.Tables)
                        {
                            for (int i = 2; i < usersDevelopmentByUserTypeDataTable.Columns.Count; i++)
                            {
                                if (usersDevelopmentByUserTypeDataTable.Columns[i].ColumnName.Trim() + (i - 2).ToString() == table.TableName)
                                {
                                    for (int j = 0; j < usersDevelopmentByUserTypeDataTable.Rows.Count; j++)
                                    {
                                        for (int k = 0; k < table.Rows.Count; k++)
                                        {
                                            if (usersDevelopmentByUserTypeDataTable.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                            {
                                                usersDevelopmentByUserTypeDataTable.Rows[j][i] = table.Rows[k][1];
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        for (int i = 2; i < usersDevelopmentByUserTypeDataTable.Columns.Count; i++)
                        {
                            int total = 0;
                            for (int j = 0; j < usersDevelopmentByUserTypeDataTable.Rows.Count; j++)
                            {
                                total += Convert.ToInt32(usersDevelopmentByUserTypeDataTable.Rows[j][i]);
                                usersDevelopmentByUserTypeDataTable.Rows[j][i] = total;

                            }
                        }

                        for (int i = 0; i < mappingsDataTable.Rows.Count; i++)
                        {
                            usersDevelopmentByMapping.Columns.Add((string)mappingsDataTable.Rows[i][1], typeof(int));
                        }

                        for (int i = 0; i < usersDevelopmentByMapping.Rows.Count; i++)
                        {
                            for (int j = 1; j < usersDevelopmentByMapping.Columns.Count; j++)
                            {
                                usersDevelopmentByMapping.Rows[i][j] = 0;
                            }
                        }

                        DataSet usersDevelopmentByMappingDataSet = new DataSet();

                        for (int i = 0; i < mappingsDataTable.Rows.Count; i++)
                        {
                            DataTable tmpMapping = new DataTable();
                            tmpMapping = (from u in usersDataTable.AsEnumerable()
                                          where u.Field<string>("MapName") == mappingsDataTable.Rows[i][1].ToString()
                                          group u by String.Format("{0:00}" + @"/", u.Field<DateTime>("CreationDate").ToString().Split('-')[1]) + u.Field<DateTime>("CreationDate").ToString().Split('-')[0] into d
                                          select new
                                          {
                                              Date = d.Key,
                                              count = d.Count()
                                          }).ToDataTable();
                            usersDevelopmentByMappingDataSet.Tables.Add(tmpMapping);
                            usersDevelopmentByMappingDataSet.Tables[i].TableName = mappingsDataTable.Rows[i][1].ToString().Trim() + i.ToString();

                        }

                        foreach (DataTable table in usersDevelopmentByMappingDataSet.Tables)
                        {
                            for (int i = 1; i < usersDevelopmentByMapping.Columns.Count; i++)
                            {
                                if (usersDevelopmentByMapping.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                                {
                                    for (int j = 0; j < usersDevelopmentByMapping.Rows.Count; j++)
                                    {
                                        for (int k = 0; k < table.Rows.Count; k++)
                                        {
                                            if (usersDevelopmentByMapping.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                            {
                                                usersDevelopmentByMapping.Rows[j][i] = table.Rows[k][1];
                                            }
                                        }
                                    }
                                }
                            }
                        }


                        for (int i = 1; i < usersDevelopmentByMapping.Columns.Count; i++)
                        {
                            int total = 0;
                            for (int j = 0; j < usersDevelopmentByMapping.Rows.Count; j++)
                            {
                                total += Convert.ToInt32(usersDevelopmentByMapping.Rows[j][i]);
                                usersDevelopmentByMapping.Rows[j][i] = total;

                            }
                        }

                        for (int i = 0; i < usersByLDAP.Rows.Count; i++)
                        {
                            usersDevelopmentByLDAP.Columns.Add((string)usersByLDAP.Rows[i][0], typeof(int));
                        }

                        for (int i = 0; i < usersDevelopmentByLDAP.Rows.Count; i++)
                        {
                            for (int j = 1; j < usersDevelopmentByLDAP.Columns.Count; j++)
                            {
                                usersDevelopmentByLDAP.Rows[i][j] = 0;
                            }
                        }

                        DataSet usersDevelopmentByLDAPDataSet = new DataSet();

                        for (int i = 0; i < usersByLDAP.Rows.Count; i++)
                        {
                            DataTable tmpUsers = new DataTable();
                            tmpUsers = (from u in usersDataTable.AsEnumerable()
                                        where u.Field<string>("LDAPDirectory") == usersByLDAP.Rows[i][0].ToString()
                                        group u by String.Format("{0:00}" + @"/", u.Field<DateTime>("CreationDate").ToString().Split('-')[1]) + u.Field<DateTime>("CreationDate").ToString().Split('-')[0] into d
                                        select new
                                        {
                                            Date = d.Key,
                                            count = d.Count()
                                        }).ToDataTable();
                            usersDevelopmentByLDAPDataSet.Tables.Add(tmpUsers);
                            usersDevelopmentByLDAPDataSet.Tables[i].TableName = usersByLDAP.Rows[i][0].ToString().Trim() + i.ToString();
                        }

                        foreach (DataTable table in usersDevelopmentByLDAPDataSet.Tables)
                        {
                            for (int i = 1; i < usersDevelopmentByLDAP.Columns.Count; i++)
                            {
                                if (usersDevelopmentByLDAP.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                                {
                                    for (int j = 0; j < usersDevelopmentByLDAP.Rows.Count; j++)
                                    {
                                        for (int k = 0; k < table.Rows.Count; k++)
                                        {
                                            if (usersDevelopmentByLDAP.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                            {
                                                usersDevelopmentByLDAP.Rows[j][i] = table.Rows[k][1];
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        for (int i = 1; i < usersDevelopmentByLDAP.Columns.Count; i++)
                        {
                            int total = 0;
                            for (int j = 0; j < usersDevelopmentByLDAP.Rows.Count; j++)
                            {
                                total += Convert.ToInt32(usersDevelopmentByLDAP.Rows[j][i]);
                                usersDevelopmentByLDAP.Rows[j][i] = total;
                            }
                        }

                        for (int i = 0; i < usersDevelopmentDataTable.Rows.Count; i++)
                        {
                            if ((string)usersDevelopmentDataTable.Rows[i][0] == "01/1")
                            {
                                    usersDevelopmentDataTable.Rows[i].Delete();
                                    usersDevelopmentByUserTypeDataTable.Rows[i].Delete();
                                    usersDevelopmentByMapping.Rows[i].Delete();
                                    usersDevelopmentByLDAP.Rows[i].Delete();
                            }
                        }

                        usersDataTable.Columns.Remove("UserID");
                        usersDataTable.Columns.Remove("UserTypeID");
                        usersDataTable.Columns.Remove("Authorizations");
                        usersDataTable.Columns.Remove("GatewayAccountAuthorizations");

                        Console.WriteLine(DateTime.Now + " Determining groups information...");



                        queryString = "Select Count([Member]) AS [Members] from Memberships where [GroupName] = $groupName and [MemberType] =  'User' union all Select Count([Member]) AS [Members] from Memberships where [GroupName] = $groupName and [MemberType] =  'Group'";
                        string queryString2 = "select count(groupname) from memberships where [member] = $groupName";

                        using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                        {
                            con.Open();
                            using (SQLiteCommand cmd = con.CreateCommand())
                            {
                                for (int i = 0; i < groupsDataTable.Rows.Count; i++)
                                {
                                    cmd.CommandText = queryString;
                                    tempDataTable = new DataTable();
                                    cmd.Parameters.AddWithValue("$groupName", groupsDataTable.Rows[i]["GroupName"]);
                                    da = new SQLiteDataAdapter(cmd);
                                    da.Fill(tempDataTable);
                                    groupsDataTable.Rows[i]["TotalUserMembers"] = tempDataTable.Rows[0][0];
                                    groupsDataTable.Rows[i]["TotalGroupMembers"] = tempDataTable.Rows[1][0];

                                    cmd.CommandText = queryString2;
                                    groupsDataTable.Rows[i]["GroupMemberships"] = Convert.ToInt32(cmd.ExecuteScalar());

                                    location = (string)groupsDataTable.Rows[i]["LocationName"];

                                    if (location != "\\")
                                    {
                                        groupsDataTable.Rows[i]["LocationName"] = location.Substring(1);
                                    }
                                    else
                                    {
                                        groupsDataTable.Rows[i]["LocationName"] = "Root";
                                    }

                                    if (Convert.ToInt32(groupsDataTable.Rows[i]["TotalUserMembers"]) > 0 && Convert.ToInt32(groupsDataTable.Rows[i]["GroupMemberships"]) == 0)
                                    {
                                        groupsDataTable.Rows[i]["Information"] = "HasMembers";
                                    }

                                    else
                                    {
                                        if (Convert.ToInt32(groupsDataTable.Rows[i]["TotalUserMembers"]) == 0 && Convert.ToInt32(groupsDataTable.Rows[i]["GroupMemberships"]) > 0)
                                        {
                                            groupsDataTable.Rows[i]["Information"] = "HasMemberships";
                                        }
                                        else
                                        {
                                            if (Convert.ToInt32(groupsDataTable.Rows[i]["TotalUserMembers"]) > 0 && Convert.ToInt32(groupsDataTable.Rows[i]["GroupMemberships"]) > 0)
                                            {
                                                groupsDataTable.Rows[i]["Information"] = "HasBoth";
                                            }
                                        }
                                    }
                                }

                            }
                            con.Close();
                        }


                        totalNumberOfGroups = groupsDataTable.Rows.Count;
                        internalGroups = (from g in groupsDataTable.AsEnumerable() where g.Field<string>("Origin") == "Internal" select g).Count();
                        ldapGroups = (from g in groupsDataTable.AsEnumerable() where g.Field<string>("Origin") == "External" select g).Count();

                        groupsByOrigin.Columns.Add("Origin", typeof(string));
                        groupsByOrigin.Columns.Add("Groups", typeof(int));
                        groupsByOrigin.Columns.Add("Share (%)", typeof(double));
                        groupsByOrigin.Rows.Add("Internal", internalGroups, Math.Round(internalGroups * 100.00 / totalNumberOfGroups, 2));
                        groupsByOrigin.Rows.Add("External", ldapGroups, Math.Round(ldapGroups * 100.00 / totalNumberOfGroups, 2));
                        groupsByOrigin.DefaultView.Sort = "Groups DESC";
                        groupsByOrigin = groupsByOrigin.DefaultView.ToTable();

                        builtinGroupMembers.Columns.Add("Group", typeof(string));
                        builtinGroupMembers.Columns.Add("UserMembers", typeof(int));
                        builtinGroupMembers.Columns.Add("InternalUserMembers", typeof(int));
                        builtinGroupMembers.Columns.Add("External User Members", typeof(int));
                        builtinGroupMembers.Columns.Add("External Group Members", typeof(int));

                        queryString = "select LDAPDirectory, Count(*) as Groups from Groups where LDAPDirectory != '' group by LDAPDirectory order by Groups desc";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(groupsByLDAP);
                        DBFunctions.closeDBConnection();

                        List<string> builtInGroups = new List<string>();
                        builtInGroups.Add("Auditors");
                        builtInGroups.Add("Notification Engines");
                        builtInGroups.Add("Backup Users");
                        builtInGroups.Add("Operators");
                        builtInGroups.Add("DR Users");
                        builtInGroups.Add("Vault Admins");
                        builtInGroups.Add("PVWAMonitor");
                        builtInGroups.Add("PVWAAppUsers");
                        builtInGroups.Add("PVWAGWAccounts");
                        builtInGroups.Add("PSMMaster");
                        builtInGroups.Add("PSMAppUsers");
                        builtInGroups.Add("PSMLiveSessionTerminators");
                        builtInGroups.Add("PSMPTAAppUsers");


                        DataTable members = new DataTable();
                        int groupID = 0;
                        DataRow[] foundGroups;
                        foreach (string builtInGroup in builtInGroups)
                        {
                            foundGroups = groupsDataTable.Select("GroupName = '" + builtInGroup.ToLower() + "'");
                            if (foundGroups != null && foundGroups.Length > 0)
                            {
                                groupID = (int)(from g in groupsDataTable.AsEnumerable() where g.Field<string>("GroupName").ToLower() == builtInGroup.ToLower() select g.Field<Int64>("GroupID")).Single();
                                members = getGroupMembersAsDataTable(groupID);
                                {
                                    builtinGroupMembers.Rows.Add(builtInGroup, (from m in members.AsEnumerable() where m.Field<string>("Type") == "User" select m).Count(), (from m in members.AsEnumerable() where m.Field<string>("Type") == "User" && m.Field<string>("Origin") == "Internal" select m).Count(), (from m in members.AsEnumerable() where m.Field<string>("Type") == "User" && m.Field<string>("Origin") == "External" select m).Count(), (from m in members.AsEnumerable() where m.Field<string>("Type") == "Group" select m).Count());
                                }
                            }
                        }
                        builtinGroupMembers.DefaultView.Sort = "UserMembers DESC";
                        builtinGroupMembers = builtinGroupMembers.DefaultView.ToTable();

                        userStatisticsDataTable.Columns.Add("Description", typeof(string));
                        userStatisticsDataTable.Columns.Add("Statistic", typeof(string));
                        userStatisticsDataTable.Columns.Add("Comments", typeof(string));
                        userStatisticsDataTable.Rows.Add("Total number of users", string.Format("{0:#,##0}", totalNumberOfUsers), "Total number of users");
                        userStatisticsDataTable.Rows.Add("Disabled users", string.Format("{0:#,##0}", disabledUsers), Math.Round(disabledUsers * 100.00 / totalNumberOfUsers, 2) + " % of all users");
                        userStatisticsDataTable.Rows.Add("Internal users", string.Format("{0:#,##0}", internalUsers), Math.Round(internalUsers * 100.00 / totalNumberOfUsers, 2) + " % of all users");
                        userStatisticsDataTable.Rows.Add("External users", string.Format("{0:#,##0}", ldapUsers), Math.Round(ldapUsers * 100.00 / totalNumberOfUsers, 2) + " % of all users");
                        userStatisticsDataTable.Rows.Add("Total number of groups", string.Format("{0:#,##0}", totalNumberOfGroups), "Total number of groups");
                        userStatisticsDataTable.Rows.Add("Internal groups", string.Format("{0:#,##0}", internalGroups), Math.Round(internalGroups * 100.00 / totalNumberOfGroups, 2) + " % of all groups");
                        userStatisticsDataTable.Rows.Add("External groups", string.Format("{0:#,##0}", ldapGroups), Math.Round(ldapGroups * 100.00 / totalNumberOfGroups, 2) + " % of all groups");
                        userStatisticsDataTable.Rows.Add("LDAP directories", string.Format("{0:#,##0}", ldapDirectoriesDataTable.Rows.Count));
                        userStatisticsDataTable.Rows.Add("Mappings", string.Format("{0:#,##0}", mappingsDataTable.Rows.Count));

                        for (int i = 0; i < usersByType.Rows.Count; i++)
                        {
                            userStatisticsDataTable.Rows.Add((string)usersByType.Rows[i][0], string.Format("{0:#,##0}", (int)usersByType.Rows[i][1]), usersByType.Rows[i][4] + " disabled");
                        }
                    }

                    monthsScale = monthDifference(minimumDate, maximumDate);

                    accountStatisticsDataTable = new DataTable();
                    accountStatisticsDataTable.Columns.Add("Description", typeof(string));
                    accountStatisticsDataTable.Columns.Add("Statistic", typeof(string));
                    accountStatisticsDataTable.Columns.Add("Comments", typeof(string));


                    // Determining accounts information...

                    accountsPerPolicyDataTable = new DataTable();
                    accountsPerPolicyDataTable.Columns.Add("PolicyID", typeof(string));
                    accountsPerPolicyDataTable.Columns.Add("NumberOfAccounts", typeof(int));
                    accountsPerPolicyDataTable.Columns.Add("Share (%)", typeof(double));
                    queryString =
                        "select objectproperties.ObjectPropertyValue as PolicyID, Count(objectproperties.ObjectPropertyValue) as NumberOfAccounts from objectproperties,files where objectproperties.ObjectPropertyName == 'PolicyID' and files.Type == 2 and files.DeletedBy == '' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID group by lower(Trim(objectproperties.ObjectPropertyValue)) order by NumberOfAccounts desc";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(accountsPerPolicyDataTable);
                    DBFunctions.closeDBConnection();

                    accountsAssignedToPolicies = accountsPerPolicyDataTable.AsEnumerable().Sum(x => x.Field<int>("NumberOfAccounts")) - tempAdHocAccounts;
                    accountsWithNoPolicyAssignment = effectiveNumberOfAccounts - accountsAssignedToPolicies;
                    numberOfPolicies = accountsPerPolicyDataTable.Rows.Count;

                    for (int i = 0; i < accountsPerPolicyDataTable.Rows.Count; i++)
                    {
                        usedPolicies.Add((string)accountsPerPolicyDataTable.Rows[i]["PolicyID"]);
                        accountsPerPolicyDataTable.Rows[i][2] = Math.Round(Int32.Parse(accountsPerPolicyDataTable.Rows[i][1].ToString()) * 100.00 / accountsAssignedToPolicies, 2);
                    }

                    if (hasPlatformPolicies && accountsPerPolicyDataTable.Rows.Count > 0)
                    {
                        platformPoliciesTable.Columns.Add("HasAccounts", typeof(string)).SetOrdinal(2);
                        platformPoliciesTable.Columns.Add("Accounts", typeof(int)).SetOrdinal(3);
                        platformPoliciesTable.Columns.Add("Allowed Safes (#)", typeof(int)).SetOrdinal(5);

                        for (int j = 0; j < platformPoliciesTable.Rows.Count; j++)
                        {
                            addedPolicies.Add((string)platformPoliciesTable.Rows[j]["PolicyID"]);
                            platformPoliciesTable.Rows[j]["HasAccounts"] = "No";
                            platformPoliciesTable.Rows[j]["Accounts"] = 0;
                            platformPoliciesTable.Rows[j]["Allowed Safes (#)"] = getNumberOfAllowedSafes((string)platformPoliciesTable.Rows[j]["AllowedSafes"]);

                            for (int i = 0; i < accountsPerPolicyDataTable.Rows.Count; i++)
                            {
                                if ((string)accountsPerPolicyDataTable.Rows[i]["PolicyID"] == (string)platformPoliciesTable.Rows[j]["PolicyID"])
                                {
                                    platformPoliciesTable.Rows[j]["HasAccounts"] = "Yes";
                                    platformPoliciesTable.Rows[j]["Accounts"] = (int)accountsPerPolicyDataTable.Rows[i]["NumberOfAccounts"];
                                    break;
                                }
                            }
                        }
                        platformPoliciesTable.DefaultView.Sort = "Accounts DESC";
                        platformPoliciesTable = platformPoliciesTable.DefaultView.ToTable();
                    }


                    usedPolicies.Remove("PSMSecureConnect");
                    missingPolicies = usedPolicies.Except(addedPolicies, StringComparer.OrdinalIgnoreCase).ToList().Where(s => !string.IsNullOrWhiteSpace(s)).OrderBy(s => s).ToList();

                    if (hasMasterPolicy)
                    {
                        otpExclusiveAccessManualChange = platformPoliciesTable.Copy();
                        for (int i = 0; i < otpExclusiveAccessManualChange.Columns.Count; i++)
                        {
                            if (otpExclusiveAccessManualChange.Columns[i].ColumnName != "Active" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "PerformPeriodicChange" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "MinValidityPeriod" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "PolicyID" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "Accounts" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "HasAccounts" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "Enforce check-in/check-out exclusive access" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "Enforce one-time password access" && otpExclusiveAccessManualChange.Columns[i].ColumnName != "AllowManualChange")
                            {
                                otpExclusiveAccessManualChange.Columns.Remove(otpExclusiveAccessManualChange.Columns[i].ColumnName);
                                i--;
                            }
                        }
           
                         otpExclusiveAccessManualChange.Columns["Enforce one-time password access"].SetOrdinal(3);
                         otpExclusiveAccessManualChange.Columns["Enforce check-in/check-out exclusive access"].SetOrdinal(4);
               

                        otpExclusiveAccessManualChange.Columns.Add("DescriptionOfExpectedBehavior", typeof(string));
                        for (int i = 0; i < otpExclusiveAccessManualChange.Rows.Count; i++)
                        {
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "Yes")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is locked when retrieved\n-If the account is released manually, the account is set for ResetImmediately=ChangeTask and the CPM will change the password based on the immediate interval\n-If the user doesn't release manually, CPM will release the account and change the password once the MinValidityPeriod has passed";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "Yes")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is locked when retrieved\n-If the account is released manually, the account is set for ResetImmediately=ChangeTask and the CPM will change the password based on the immediate interval\n-If the user doesn't release manually the account will stay locked";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "No")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is locked when retrieved\n-If the account is released manually, the password won't change\n-If the account isn't released manually, the account will be released after the MinValidityPeriod";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "No")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is locked when retrieved\n-CPM will never change the password (when being in this mode, but the password changes, it can be an idea to uncheck AllowManualChange)";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "Yes")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is NOT locked when retrieved\n-The password will be changed by the MinValidityPeriod\n-If the policy is set to periodic change, the password will change in the periodic cycle";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Active" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "No")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is NOT locked when retrieved\n-The password will NOT change by MinValidityPeriod because the one-time change requires AllowManualChange to be set to 'yes'. The account will be found, but ignored\n-If the policy is set to periodic change, the password will change in the periodic cycle";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "No")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is NOT locked when retrieved\n-The password will NOT change by MinValidityPeriod because both one-time passwords and AllowManualChange are off\n-If the policy is set to periodic change, the password will change in the periodic cycle";
                            }
                            if ((string)otpExclusiveAccessManualChange.Rows[i]["Enforce check-in/check-out exclusive access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["Enforce one-time password access"] == "Inactive" && (string)otpExclusiveAccessManualChange.Rows[i]["AllowManualChange"] == "Yes")
                            {
                                otpExclusiveAccessManualChange.Rows[i]["DescriptionOfExpectedBehavior"] = "-Account is NOT locked when retrieved\n-The password will NOT change by the MinValidityPeriod because one-time password is inactive\n-If the policy is set to periodic change, the password will change in the periodic cycle";
                            }
                        }

							otpExclusiveAccessManualChange.Columns["Enforce check-in/check-out exclusive access"].ColumnName = "ExclusiveAccess";
                            otpExclusiveAccessManualChange.Columns["Enforce one-time password access"].ColumnName = "One-timePassword";
                            otpExclusiveAccessManualChange.Columns["PerformPeriodicChange"].ColumnName = "PeriodicChange";
                            otpExclusiveAccessManualChange.Columns["AllowManualChange"].ColumnName = "ManualChange";
                            otpExclusiveAccessManualChange.DefaultView.Sort = "DescriptionOfExpectedBehavior";
                            otpExclusiveAccessManualChange = otpExclusiveAccessManualChange.DefaultView.ToTable();
             
                    }



                    Dictionary<string, HashSet<string>> policiesAllowedSafes = new Dictionary<string, HashSet<string>>();
                    if (addedPolicies.Count > 0)
                    {
                        foreach (string policy in addedPolicies.Intersect(usedPolicies))
                        {
                            policiesAllowedSafes.Add(policy, getAllowedSafes(policy));
                        }
                    }

                    accountsPerDeviceTypeDataTable = new DataTable();
                    accountsPerDeviceTypeDataTable.Columns.Add("DeviceType", typeof(string));
                    accountsPerDeviceTypeDataTable.Columns.Add("NumberOfAccounts", typeof(int));
                    accountsPerDeviceTypeDataTable.Columns.Add("Share (%)", typeof(double));
                    queryString =
                        "select objectproperties.ObjectPropertyValue as DeviceType, Count(objectproperties.ObjectPropertyValue) as NumberOfAccounts from objectproperties,files where objectproperties.ObjectPropertyName == 'DeviceType' and files.Type == 2 and files.DeletedBy == '' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID group by lower(objectproperties.ObjectPropertyValue) order by NumberOfAccounts desc";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(accountsPerDeviceTypeDataTable);
                    DBFunctions.closeDBConnection();
                    for (int i = 0; i < accountsPerDeviceTypeDataTable.Rows.Count; i++)
                    {
                        accountsPerDeviceTypeDataTable.Rows[i][2] = Math.Round(Int32.Parse(accountsPerDeviceTypeDataTable.Rows[i][1].ToString()) * 100.00 / accountsAssignedToPolicies, 2);
                    }


                    queryString = "select SafeName, Folder, SafeID || '_' || FileID as AccountID, AccountName, UserName, Address, Trim(PolicyID) as PolicyID, DeviceType, Case When (DisabledReason is not null) then 'Yes' Else 'No' End as 'Disabled', DisabledReason, Case When (DisabledReason Like '(CPM)%' And DisabledReason is not null) then 'CPM' When (DisabledReason Not Like '(CPM)%' And DisabledReason is not null) then 'User' End as 'DisabledBy', CPMStatus, LastTask, CPMErrorDetails, LastFailDate, LastSuccessChange, LastSuccessReconciliation, LastSuccessVerification, LogonAccount, ReconcileAccount, EnableAccount, CASE WHEN (CreationMethod IS NULL AND SafeName Not Like '%_workspace') then 'PrivateArk/PACLI' When (CreationMethod = 'PVWA' AND SafeName Not Like '%_workspace') then 'PVWA/REST API' Else CreationMethod End as CreationMethod, CreatedBy, CreationDate, CASE WHEN (DeletedBy == '') then 'No' Else 'Yes' End as 'Deleted', DeletedBy, CASE WHEN DeletionDate == '0001-01-01 00:00:00' THEN Null ELSE DeletionDate END as DeletionDate, LastUsedBy, LastUsedDate, LastModifiedBy, CASE WHEN LastModifiedDate == '0001-01-01 00:00:00' THEN Null ELSE LastModifiedDate END as LastModifiedDate, LastUsedByHuman, CASE WHEN LastUsedHumanDate == '0001-01-01 00:00:00' THEN Null ELSE LastUsedHumanDate END as LastUsedHumanDate, LastUsedByComponent, CASE WHEN LastUsedComponentDate == '0001-01-01 00:00:00' THEN Null ELSE LastUsedComponentDate END as LastUsedComponentDate from ((((((((select files.FileID, files.SafeID, files.FileName as AccountName, files.SafeName, files.Folder, files.CreatedBy, files.CreationDate, files.DeletedBy, files.DeletionDate, files.LastUsedBy, files.LastUsedDate, files.LastModifiedBy, files.LastModifiedDate, files.LastUsedByHuman, files.LastUsedHumanDate, files.LastUsedByComponent, files.LastUsedComponentDate from files where files.type == 2) t1 left join (select objectproperties.FileID, objectproperties.SafeID, Trim(objectproperties.ObjectPropertyValue) as 'PolicyID' from objectproperties where objectproperties.ObjectPropertyName == 'PolicyID') using (FileID,SafeID))) left join (select objectproperties.FileID, objectproperties.SafeID,  objectproperties.ObjectPropertyValue as 'DeviceType' from objectproperties where objectproperties.ObjectPropertyName == 'DeviceType') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'UserName' from objectproperties where objectproperties.ObjectPropertyName == 'UserName') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'Address' from objectproperties where objectproperties.ObjectPropertyName == 'Address') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CPMStatus' from objectproperties where objectproperties.ObjectPropertyName == 'CPMStatus') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CPMErrorDetails' from objectproperties where objectproperties.ObjectPropertyName == 'CPMErrorDetails') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'DisabledReason' from objectproperties where objectproperties.ObjectPropertyName == 'CPMDisabled') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LogonAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass1Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'ReconcileAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass3Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'EnableAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass2Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CreationMethod' from objectproperties where objectproperties.ObjectPropertyName == 'CreationMethod') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastTask' from objectproperties where objectproperties.ObjectPropertyName == 'LastTask') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessVerification' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessVerification') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessChange' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessChange') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastFailDate' from objectproperties where objectproperties.ObjectPropertyName == 'LastFailDate') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessReconciliation' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessReconciliation') using (FileID,SafeID) order by AccountName asc";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(accountsDataTable);
                    DBFunctions.closeDBConnection();

                    tempDataTable = new DataTable();
                    queryString = "Create table Accounts as select SafeName, Folder, AccountName, UserName, Address, Trim(PolicyID) as PolicyID, DeviceType, Case When (DisabledReason is not null) then 'Yes' Else 'No' End as 'Disabled', DisabledReason, Case When (DisabledReason Like '(CPM)%' And DisabledReason is not null) then 'CPM' When (DisabledReason Not Like '(CPM)%' And DisabledReason is not null) then 'User' End as 'DisabledBy', CPMStatus, LastTask, CPMErrorDetails, LastFailDate, LastSuccessChange, LastSuccessReconciliation, LastSuccessVerification, LogonAccount, ReconcileAccount, EnableAccount, CASE WHEN (CreationMethod IS NULL AND SafeName Not Like '%_workspace') then 'PrivateArk/PACLI' When (CreationMethod = 'PVWA' AND SafeName Not Like '%_workspace') then 'PVWA/REST API' Else CreationMethod End as CreationMethod, CreatedBy, CreationDate, CASE WHEN (DeletedBy == '') then 'No' Else 'Yes' End as 'Deleted', DeletedBy, CASE WHEN DeletionDate == '0001-01-01 00:00:00' THEN Null ELSE DeletionDate END as DeletionDate, LastUsedBy, LastUsedDate, LastModifiedBy, CASE WHEN LastModifiedDate == '0001-01-01 00:00:00' THEN Null ELSE LastModifiedDate END as LastModifiedDate, LastUsedByHuman, CASE WHEN LastUsedHumanDate == '0001-01-01 00:00:00' THEN Null ELSE LastUsedHumanDate END as LastUsedHumanDate, LastUsedByComponent, CASE WHEN LastUsedComponentDate == '0001-01-01 00:00:00' THEN Null ELSE LastUsedComponentDate END as LastUsedComponentDate from ((((((((select files.FileID, files.SafeID, files.FileName as AccountName, files.SafeName, files.Folder, files.CreatedBy, files.CreationDate, files.DeletedBy, files.DeletionDate, files.LastUsedBy, files.LastUsedDate, files.LastModifiedBy, files.LastModifiedDate, files.LastUsedByHuman, files.LastUsedHumanDate, files.LastUsedByComponent, files.LastUsedComponentDate from files where files.type == 2 and files.DeletedBy = '' and files.SafeName not like '%_workspace%' and files.SafeName != 'PSMUnmanagedSessionAccounts') t1 left join (select objectproperties.FileID, objectproperties.SafeID, Trim(objectproperties.ObjectPropertyValue) as 'PolicyID' from objectproperties where objectproperties.ObjectPropertyName == 'PolicyID') using (FileID,SafeID))) left join (select objectproperties.FileID, objectproperties.SafeID,  objectproperties.ObjectPropertyValue as 'DeviceType' from objectproperties where objectproperties.ObjectPropertyName == 'DeviceType') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'UserName' from objectproperties where objectproperties.ObjectPropertyName == 'UserName') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'Address' from objectproperties where objectproperties.ObjectPropertyName == 'Address') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CPMStatus' from objectproperties where objectproperties.ObjectPropertyName == 'CPMStatus') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CPMErrorDetails' from objectproperties where objectproperties.ObjectPropertyName == 'CPMErrorDetails') using (FileID,SafeID)) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'DisabledReason' from objectproperties where objectproperties.ObjectPropertyName == 'CPMDisabled') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LogonAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass1Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'ReconcileAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass3Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'EnableAccount' from objectproperties where objectproperties.ObjectPropertyName == 'ExtraPass2Name') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CreationMethod' from objectproperties where objectproperties.ObjectPropertyName == 'CreationMethod') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastTask' from objectproperties where objectproperties.ObjectPropertyName == 'LastTask') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessVerification' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessVerification') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessChange' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessChange') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastFailDate' from objectproperties where objectproperties.ObjectPropertyName == 'LastFailDate') using (FileID,SafeID) left join (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'LastSuccessReconciliation' from objectproperties where objectproperties.ObjectPropertyName == 'LastSuccessReconciliation') using (FileID,SafeID)";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(tempDataTable);
                    DBFunctions.closeDBConnection();

                    tempDataTable = new DataTable();
                    queryString = "Create INDEX if not exists i_PolicyID ON Accounts(PolicyID);";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(tempDataTable);
                    DBFunctions.closeDBConnection();

                    tempDataTable = new DataTable();
                    queryString = "Create INDEX if not exists i_DeviceType ON Accounts(DeviceType);";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(tempDataTable);
                    DBFunctions.closeDBConnection();

                    tempDataTable = new DataTable();
                    queryString = "Create INDEX if not exists i_CreationMethod ON Accounts(CreationMethod);";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(tempDataTable);
                    DBFunctions.closeDBConnection();

                    accountsByCreationMethod = (from accounts in accountsDataTable.AsEnumerable()
                                                group accounts by accounts.Field<string>("CreationMethod") into d
                                                orderby d.Count() descending
                                                select new
                                                {
                                                    CreationMethod = d.Key,
                                                    Accounts = d.Count()
                                                }).ToDataTable();
                    accountsByCreationMethod.Columns.Add("Share (%)", typeof(double));
                    for (int i = 0; i < accountsByCreationMethod.Rows.Count; i++)
                    {
                        if (accountsByCreationMethod.Rows[i][0].ToString() == "")
                        {
                            accountsByCreationMethod.Rows[i].Delete();
                        }
                    }
                    for (int i = 0; i < accountsByCreationMethod.Rows.Count; i++)
                    {
                        accountsByCreationMethod.Rows[i]["Share (%)"] = Math.Round((int)accountsByCreationMethod.Rows[i]["Accounts"] * 100.00 / accountsByCreationMethod.AsEnumerable().Sum(x => x.Field<int>("Accounts")), 2);
                    }

                    accountsDataTable.Columns.Add("PeriodicChange", typeof(string)).SetOrdinal(accountsDataTable.Columns["DisabledBy"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Owned by CPM(s)", typeof(string)).SetOrdinal(accountsDataTable.Columns["PeriodicChange"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Allowed for CPM(s)", typeof(string)).SetOrdinal(accountsDataTable.Columns["Owned by CPM(s)"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Managed", typeof(string)).SetOrdinal(accountsDataTable.Columns["Allowed for CPM(s)"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Rotation interval", typeof(int)).SetOrdinal(accountsDataTable.Columns["Managed"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Change days ago", typeof(int)).SetOrdinal(accountsDataTable.Columns["Rotation interval"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Reconcile days ago", typeof(int)).SetOrdinal(accountsDataTable.Columns["Change days ago"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Verify days ago", typeof(int)).SetOrdinal(accountsDataTable.Columns["LastSuccessVerification"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Compliant", typeof(string)).SetOrdinal(accountsDataTable.Columns["Reconcile days ago"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Days non-compliant", typeof(int)).SetOrdinal(accountsDataTable.Columns["Compliant"].Ordinal + 1);
                    accountsDataTable.Columns.Add("Temp1", typeof(DateTime)); //LastSuccessChange
                    accountsDataTable.Columns.Add("Temp2", typeof(DateTime)); //LastSuccessVerification
                    accountsDataTable.Columns.Add("Temp3", typeof(DateTime)); //LastFailDate
                    accountsDataTable.Columns.Add("Temp4", typeof(DateTime)); //DeletionDate
                    accountsDataTable.Columns.Add("Temp5", typeof(DateTime)); //LastModifiedDate
                    accountsDataTable.Columns.Add("Temp6", typeof(DateTime)); //LastUsedHumanDate
                    accountsDataTable.Columns.Add("Temp7", typeof(DateTime)); //LastUsedComponentDate
                    accountsDataTable.Columns.Add("Temp8", typeof(DateTime)); //LastSuccessReconciliation

                    managedAccountsByPolicySortedByManaged = accountsPerPolicyDataTable.Copy();
                    managedAccountsByPolicySortedByManaged.Columns.Remove("Share (%)");

                    managedAccountsByPolicySortedByManaged.Columns.Add("Managed", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Unmanaged", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("n/a - missing platform information", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Compliant", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Non-Compliant", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Compliant n/a", typeof(int));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Managed (%)", typeof(double));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Unmanaged (%)", typeof(double));
                    managedAccountsByPolicySortedByManaged.Columns.Add("n/a - missing platform information (%)", typeof(double));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Compliant (%)", typeof(double));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Non-Compliant (%)", typeof(double));
                    managedAccountsByPolicySortedByManaged.Columns.Add("Average days non-compliant", typeof(int));



                    for (int i = 0; i < managedAccountsByPolicySortedByManaged.Rows.Count; i++)
                    {
                        managedAccountsByPolicySortedByManaged.Rows[i]["Managed"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Managed (%)"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Unmanaged"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Unmanaged (%)"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["n/a - missing platform information"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["n/a - missing platform information (%)"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Compliant"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Compliant (%)"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant (%)"] = 0;
                        managedAccountsByPolicySortedByManaged.Rows[i]["Compliant n/a"] = 0;
                    }

                    managedAccountsByDeviceTypeSortedByManaged = accountsPerDeviceTypeDataTable.Copy();
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Remove("Share (%)");

                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Managed", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Unmanaged", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("n/a - missing platform information", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Compliant", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Non-Compliant", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Compliant n/a", typeof(int));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Managed (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Unmanaged (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("n/a - missing platform information (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Compliant (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Non-Compliant (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Compliant n/a (%)", typeof(double));
                    managedAccountsByDeviceTypeSortedByManaged.Columns.Add("Average days non-compliant", typeof(int));

                    for (int i = 0; i < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; i++)
                    {
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Managed"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Managed (%)"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Unmanaged"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Unmanaged (%)"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["n/a - missing platform information"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["n/a - missing platform information (%)"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Compliant"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Compliant (%)"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant (%)"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Compliant n/a"] = 0;
                        managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Compliant n/a (%)"] = 0;
                    }

                    accountDevelopmentByPolicyCreated = accountsGroupedByCreationDate.Copy();
                    accountDevelopmentByPolicyCreated.Columns["dt"].ColumnName = "Date";
                    accountDevelopmentByPolicyCreated.Columns.Remove("count");
                    accountDevelopmentByPolicyCreated.Columns.Remove("sorter");
                    accountDevelopmentByPolicyCreated.Columns.Remove("TotalSize");

                    DataTable tmpTable = new DataTable();

                    tmpTable = accountsPerPolicyDataTable.Copy();
                    accountDevelopmentByDeviceTypeCreated = accountDevelopmentByPolicyCreated.Copy();
                    accountDevelopmentByCreationMethodCreated = accountDevelopmentByPolicyCreated.Copy();
                    for (int i = 0; i < accountsPerPolicyDataTable.Rows.Count; i++)
                    {
                        accountDevelopmentByPolicyCreated.Columns.Add(tmpTable.Rows[i][0].ToString(), typeof(int));
                    }
                    accountDevelopmentByPolicyDeleted = accountDevelopmentByPolicyCreated.Copy();
                    accountDevelopmentByPolicy = accountDevelopmentByPolicyCreated.Copy();

                    tmpTable = accountsPerDeviceTypeDataTable.Copy();
                    for (int i = 0; i < accountsPerDeviceTypeDataTable.Rows.Count; i++)
                    {
                        accountDevelopmentByDeviceTypeCreated.Columns.Add(tmpTable.Rows[i][0].ToString(), typeof(int));
                    }
                    accountDevelopmentByDeviceTypeDeleted = accountDevelopmentByDeviceTypeCreated.Copy();
                    accountDevelopmentByDeviceType = accountDevelopmentByDeviceTypeCreated.Copy();

                    tmpTable = accountsByCreationMethod.Copy();
                    for (int i = 0; i < accountsByCreationMethod.Rows.Count; i++)
                    {
                        accountDevelopmentByCreationMethodCreated.Columns.Add(tmpTable.Rows[i][0].ToString(), typeof(int));
                    }
                    accountDevelopmentByCreationMethodDeleted = accountDevelopmentByCreationMethodCreated.Copy();
                    accountDevelopmentByCreationMethod = accountDevelopmentByCreationMethodCreated.Copy();

                    DataSet accountsDevelopmentByPolicyCreated = new DataSet();
                    DataSet accountsDevelopmentByPolicyDeleted = new DataSet();
                    DataSet accountsDevelopmentByDeviceTypeCreated = new DataSet();
                    DataSet accountsDevelopmentByDeviceTypeDeleted = new DataSet();
                    DataSet accountsDevelopmentByCreationMethodCreated = new DataSet();
                    DataSet accountsDevelopmentByCreationMethodDeleted = new DataSet();

					// Determining accounts development...

                    using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                    {
                        con.Open();
                        using (SQLiteCommand cmd = con.CreateCommand())
                        {

                            for (int i = 0; i < accountsPerPolicyDataTable.Rows.Count; i++)
                            {
                                cmd.CommandText = "select strftime('%m/%Y',CreationDate) as Time, Count(*) as Count from accounts where PolicyID = $policy group by Time";
                                DataTable tmpCreated = new DataTable();
                                cmd.Parameters.AddWithValue("$policy", accountsPerPolicyDataTable.Rows[i][0].ToString());
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpCreated);
                                accountsDevelopmentByPolicyCreated.Tables.Add(tmpCreated);
                                accountsDevelopmentByPolicyCreated.Tables[i].TableName = accountsPerPolicyDataTable.Rows[i][0].ToString().Trim() + i.ToString();

                                cmd.CommandText = "select strftime('%m/%Y',DeletionDate) as Time, Count(*) as Count from accounts where Deleted = 'Yes' and PolicyID = $policy group by Time";
                                DataTable tmpDeleted = new DataTable();
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpDeleted);
                                accountsDevelopmentByPolicyDeleted.Tables.Add(tmpDeleted);
                                accountsDevelopmentByPolicyDeleted.Tables[i].TableName = accountsPerPolicyDataTable.Rows[i][0].ToString().Trim() + i.ToString();
                            }

                            for (int i = 0; i < accountsPerDeviceTypeDataTable.Rows.Count; i++)
                            {
                                cmd.CommandText = "select strftime('%m/%Y',CreationDate) as Time, Count(*) as Count from accounts where DeviceType = $deviceType group by Time";
                                DataTable tmpCreated = new DataTable();
                                cmd.Parameters.AddWithValue("$deviceType", accountsPerDeviceTypeDataTable.Rows[i][0].ToString());
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpCreated);
                                accountsDevelopmentByDeviceTypeCreated.Tables.Add(tmpCreated);
                                accountsDevelopmentByDeviceTypeCreated.Tables[i].TableName = accountsPerDeviceTypeDataTable.Rows[i][0].ToString().Trim() + i.ToString();

                                cmd.CommandText = "select strftime('%m/%Y',DeletionDate) as Time, Count(*) as Count from accounts where Deleted = 'Yes' and DeviceType = $deviceType group by Time";
                                DataTable tmpDeleted = new DataTable();
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpDeleted);
                                accountsDevelopmentByDeviceTypeDeleted.Tables.Add(tmpDeleted);
                                accountsDevelopmentByDeviceTypeDeleted.Tables[i].TableName = accountsPerDeviceTypeDataTable.Rows[i][0].ToString().Trim() + i.ToString();
                            }

                            for (int i = 0; i < accountsByCreationMethod.Rows.Count; i++)
                            {
                                cmd.CommandText = "select strftime('%m/%Y',CreationDate) as Time, Count(*) as Count from accounts where CreationMethod = $creationMethod group by Time";
                                DataTable tmpCreated = new DataTable();
                                cmd.Parameters.AddWithValue("$creationMethod", accountsByCreationMethod.Rows[i][0].ToString());
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpCreated);
                                accountsDevelopmentByCreationMethodCreated.Tables.Add(tmpCreated);
                                accountsDevelopmentByCreationMethodCreated.Tables[i].TableName = accountsByCreationMethod.Rows[i][0].ToString().Trim() + i.ToString();

                                cmd.CommandText = "select strftime('%m/%Y',DeletionDate) as Time, Count(*) as Count from accounts where Deleted = 'Yes' and CreationMethod = $creationMethod group by Time";
                                DataTable tmpDeleted = new DataTable();
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpDeleted);
                                accountsDevelopmentByCreationMethodDeleted.Tables.Add(tmpDeleted);
                                accountsDevelopmentByCreationMethodDeleted.Tables[i].TableName = accountsByCreationMethod.Rows[i][0].ToString().Trim() + i.ToString();
                            }
                        }
                        con.Close();
                    }


                    foreach (DataTable table in accountsDevelopmentByPolicyCreated.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByPolicyCreated.Columns.Count; i++)
                        {
                            if (accountDevelopmentByPolicyCreated.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByPolicyCreated.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByPolicyCreated.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByPolicyCreated.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    foreach (DataTable table in accountsDevelopmentByPolicyDeleted.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByPolicyDeleted.Columns.Count; i++)
                        {
                            if (accountDevelopmentByPolicyDeleted.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByPolicyDeleted.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByPolicyDeleted.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByPolicyDeleted.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    for (int i = 1; i < accountDevelopmentByPolicyCreated.Columns.Count; i++)
                    {
                        int total = 0;

                        for (int j = 0; j < accountDevelopmentByPolicyCreated.Rows.Count; j++)
                        {
                            if (accountDevelopmentByPolicyCreated.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByPolicyCreated.Rows[j][i] = 0;
                            }
                            if (accountDevelopmentByPolicyDeleted.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByPolicyDeleted.Rows[j][i] = 0;
                            }

                            total = total + Convert.ToInt32(accountDevelopmentByPolicyCreated.Rows[j][i].ToString()) - Convert.ToInt32(accountDevelopmentByPolicyDeleted.Rows[j][i].ToString());
                            accountDevelopmentByPolicy.Rows[j][i] = total;
                        }
                    }

                    foreach (DataTable table in accountsDevelopmentByDeviceTypeCreated.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByDeviceTypeCreated.Columns.Count; i++)
                        {
                            if (accountDevelopmentByDeviceTypeCreated.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByDeviceTypeCreated.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByDeviceTypeCreated.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByDeviceTypeCreated.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    foreach (DataTable table in accountsDevelopmentByDeviceTypeDeleted.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByDeviceTypeDeleted.Columns.Count; i++)
                        {
                            if (accountDevelopmentByDeviceTypeDeleted.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByDeviceTypeDeleted.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByDeviceTypeDeleted.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByDeviceTypeDeleted.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    for (int i = 1; i < accountDevelopmentByDeviceTypeCreated.Columns.Count; i++)
                    {
                        int total = 0;

                        for (int j = 0; j < accountDevelopmentByDeviceTypeCreated.Rows.Count; j++)
                        {
                            if (accountDevelopmentByDeviceTypeCreated.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByDeviceTypeCreated.Rows[j][i] = 0;
                            }
                            if (accountDevelopmentByDeviceTypeDeleted.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByDeviceTypeDeleted.Rows[j][i] = 0;
                            }

                            total = total + Convert.ToInt32(accountDevelopmentByDeviceTypeCreated.Rows[j][i].ToString()) - Convert.ToInt32(accountDevelopmentByDeviceTypeDeleted.Rows[j][i].ToString());
                            accountDevelopmentByDeviceType.Rows[j][i] = total;
                        }
                    }

                    foreach (DataTable table in accountsDevelopmentByCreationMethodCreated.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByCreationMethodCreated.Columns.Count; i++)
                        {
                            if (accountDevelopmentByCreationMethodCreated.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByCreationMethodCreated.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByCreationMethodCreated.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByCreationMethodCreated.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    foreach (DataTable table in accountsDevelopmentByCreationMethodDeleted.Tables)
                    {
                        for (int i = 1; i < accountDevelopmentByCreationMethodDeleted.Columns.Count; i++)
                        {
                            if (accountDevelopmentByCreationMethodDeleted.Columns[i].ColumnName.Trim() + (i - 1).ToString() == table.TableName)
                            {
                                for (int j = 0; j < accountDevelopmentByCreationMethodDeleted.Rows.Count; j++)
                                {
                                    for (int k = 0; k < table.Rows.Count; k++)
                                    {
                                        if (accountDevelopmentByCreationMethodDeleted.Rows[j][0].ToString() == table.Rows[k][0].ToString())
                                        {
                                            accountDevelopmentByCreationMethodDeleted.Rows[j][i] = table.Rows[k][1];
                                            break;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }

                    for (int i = 1; i < accountDevelopmentByCreationMethodCreated.Columns.Count; i++)
                    {
                        int total = 0;

                        for (int j = 0; j < accountDevelopmentByCreationMethodCreated.Rows.Count; j++)
                        {
                            if (accountDevelopmentByCreationMethodCreated.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByCreationMethodCreated.Rows[j][i] = 0;
                            }
                            if (accountDevelopmentByCreationMethodDeleted.Rows[j][i].ToString() == "")
                            {
                                accountDevelopmentByCreationMethodDeleted.Rows[j][i] = 0;
                            }

                            total = total + Convert.ToInt32(accountDevelopmentByCreationMethodCreated.Rows[j][i].ToString()) - Convert.ToInt32(accountDevelopmentByCreationMethodDeleted.Rows[j][i].ToString());
                            accountDevelopmentByCreationMethod.Rows[j][i] = total;
                        }
                    }


                    if (hasMasterPolicy && hasPlatformPolicies)
                    {
                        HashSet<string> allowedSafesHashset = new HashSet<string>();

                        Console.WriteLine(DateTime.Now + " Determining accounts management and compliance...");

                        Dictionary<string, string[]> policyInformationDictionary = new Dictionary<string, string[]>();
                        policyInformationDictionary = platformPoliciesTable.AsEnumerable().DistinctBy(row => row.Field<string>("PolicyID").ToUpper()).ToDictionary<DataRow, string, string[]>(row => row.Field<string>("PolicyID").ToUpper(), row => (row.Field<string>("PolicyID").ToUpper() + ";" + row.Field<string>("PerformPeriodicChange") + ";" + row.Field<string>("Require Password Change every X days")).Split(';'));

                        Dictionary<string, string> allowedSafesCheck = new Dictionary<string, string>();
                        string isAllowedString = string.Empty;
                        string[] policyInformation = new string[3];
                        List<string> searchedPolicies = new List<string>();

                        for (int i = 0; i < accountsDataTable.Rows.Count; i++)
                        {
                            if (policyInformationDictionary.TryGetValue(accountsDataTable.Rows[i]["PolicyID"].ToString().ToUpper(), out policyInformation))
                            {
                                accountsDataTable.Rows[i]["PeriodicChange"] = policyInformation[1];
                                accountsDataTable.Rows[i]["Rotation interval"] = Convert.ToInt32(policyInformation[2]);

                                if (policyInformation[1] == "Yes" && accountsDataTable.Rows[i]["DeletedBy"].ToString() == "")
                                {
                                    accountsDataTable.Rows[i]["Managed"] = "Yes";
                                }
                                else
                                {
                                    accountsDataTable.Rows[i]["Managed"] = "No";
                                    accountsDataTable.Rows[i]["PeriodicChange"] = "No";
                                }
                            }
                            else if (!searchedPolicies.Contains(accountsDataTable.Rows[i]["PolicyID"].ToString()) && !skipDeterminingPolicyInformation && !MainWindow.skipRestApiActions && settings.getAdditionalPolicyInformation && MainWindow.restApiSession != null && MainWindow.restApiSession.sessionToken != null && MainWindow.restApiSession.apiVersion != null)
                            {
                                policyInformation = await RestApiFunctions.getPolicyComplianceInformation(MainWindow.restApiSession, accountsDataTable.Rows[i]["PolicyID"].ToString());
                                searchedPolicies.Add(accountsDataTable.Rows[i]["PolicyID"].ToString());
                                if (policyInformation != null)
                                {
                                    if (missingPolicies != null)
                                    {
                                        missingPolicies.Remove(accountsDataTable.Rows[i]["PolicyID"].ToString());
                                    }

                                    if (!policyInformationDictionary.ContainsKey(accountsDataTable.Rows[i]["PolicyID"].ToString()))
                                    {
                                        policyInformationDictionary.Add(accountsDataTable.Rows[i]["PolicyID"].ToString(), policyInformation);
                                    }

                                    accountsDataTable.Rows[i]["PeriodicChange"] = policyInformation[1];
                                    accountsDataTable.Rows[i]["Rotation interval"] = Convert.ToInt32(policyInformation[2]);

                                    if (policyInformation[1] == "Yes" && accountsDataTable.Rows[i]["DeletedBy"].ToString() == "")
                                    {
                                        accountsDataTable.Rows[i]["Managed"] = "Yes";
                                    }
                                    else
                                    {
                                        accountsDataTable.Rows[i]["Managed"] = "No";
                                        accountsDataTable.Rows[i]["PeriodicChange"] = "No";
                                    }
                                }
                            }

                            if (accountsDataTable.Rows[i]["PolicyID"].ToString() == "" || accountsDataTable.Rows[i]["DisabledBy"].ToString() == "User")
                            {
                                accountsDataTable.Rows[i]["Managed"] = "No";
                            }

                            if (accountsDataTable.Rows[i]["PolicyID"] != DBNull.Value && allowedSafesCheck.TryGetValue((string)accountsDataTable.Rows[i]["PolicyID"], out isAllowedString))
                            {
                                if (isAllowedString == "Yes")
                                {
                                    accountsDataTable.Rows[i]["Allowed for CPM(s)"] = "Yes";
                                }
                                else
                                {
                                    accountsDataTable.Rows[i]["Allowed for CPM(s)"] = "No";
                                    accountsDataTable.Rows[i]["Managed"] = "No";
                                }
                            }
                            else if (accountsDataTable.Rows[i]["PolicyID"] == DBNull.Value)
                            {
                                accountsDataTable.Rows[i]["Allowed for CPM(s)"] = "Yes";
                            }
                            else if (policiesAllowedSafes.TryGetValue((string)accountsDataTable.Rows[i]["PolicyID"], out allowedSafesHashset))
                            {
                                if (allowedSafesHashset.Contains(accountsDataTable.Rows[i]["SafeName"]))
                                {
                                    allowedSafesCheck.Add((string)accountsDataTable.Rows[i]["PolicyID"], "Yes");
                                    accountsDataTable.Rows[i]["Allowed for CPM(s)"] = "Yes";
                                }
                                else
                                {
                                    allowedSafesCheck.Add((string)accountsDataTable.Rows[i]["PolicyID"], "No");
                                    accountsDataTable.Rows[i]["Allowed for CPM(s)"] = "No";
                                    accountsDataTable.Rows[i]["Managed"] = "No";
                                }
                            }

                        }
                    }


                    int managedSuccess = 0;
                    int managedFailure = 0;
                    int managedCPMdisabled = 0;
                    int nonCompliantSuccess = 0;
                    int nonCompliantFailure = 0;
                    int nonCompliantCPMdisabled = 0;
                    bool hasCPMs = false;

                    Dictionary<string, HashSet<string>> cpmOwnerships = new Dictionary<string, HashSet<string>>();
                    if (CPMs.Count > 0)
                    {
                        hasCPMs = true;
                        if (CPMs.Count > 1)
                        {
                            CPMs.Add("Several CPMs");
                        }

                        CPMs.Add("No CPM");

                        cpmOwnerships = getSafeOwnerships("", CPMs);
                        for (int i = 0; i < CPMs.Count; i++)
                        {
                            accountsDataTable.Columns.Add(CPMs[i], typeof(string));
                        }
                    }

                    for (int i = 0; i < accountsDataTable.Rows.Count; i++)
                    {


                        if (hasCPMs)
                        {
                            bool ownedByCPM = false;
                            int cpmOwnersCount = 0;
                            for (int c = 0; c < CPMs.Count; c++)
                            {
                                HashSet<string> safesList = cpmOwnerships[CPMs[c]];
                                if (safesList.Contains((string)accountsDataTable.Rows[i]["SafeName"]))
                                {
                                    accountsDataTable.Rows[i][CPMs[c]] = "Yes";
                                    ownedByCPM = true;
                                    cpmOwnersCount++;
                                }
                                else
                                {
                                    accountsDataTable.Rows[i][CPMs[c]] = "No";
                                }
                            }
                            if (ownedByCPM)
                            {
                                accountsDataTable.Rows[i]["No CPM"] = "No";
                                accountsDataTable.Rows[i]["Owned by CPM(s)"] = "Yes";

                                if (cpmOwnersCount > 1 && CPMs.Count() > 2)
                                {
                                    accountsDataTable.Rows[i]["Several CPMs"] = "Yes";
                                }
                                else if (cpmOwnersCount < 2 && CPMs.Count() > 2)
                                {
                                    accountsDataTable.Rows[i]["Several CPMs"] = "No";
                                }
                            }
                            else
                            {
                                accountsDataTable.Rows[i]["No CPM"] = "Yes";
                                accountsDataTable.Rows[i]["Owned by CPM(s)"] = "No";
                                accountsDataTable.Rows[i]["Managed"] = "No";
                          
                            }
                        }

                        if (accountsDataTable.Rows[i]["Deleted"].ToString() == "Yes")
                        {
                            accountsDataTable.Rows[i]["Managed"] = "Deleted Account";
                            accountsDataTable.Rows[i]["Temp4"] = accountsDataTable.Rows[i]["DeletionDate"];

                        }
                        else
                        {
                            if (accountsDataTable.Rows[i]["DisabledBy"].ToString() == "CPM")
                            {
                                cpmDisabledAccounts += 1;
                            }
                            if (accountsDataTable.Rows[i]["DisabledBy"].ToString() == "User")
                            {
                                userDisabledAccounts += 1;
                            }

                            if (accountsDataTable.Rows[i]["SafeName"].ToString().ToLower().Contains("_workspace"))
                            {
                                accountsDataTable.Rows[i]["Managed"] = "Temporary Account";
                            }

                            if (accountsDataTable.Rows[i]["SafeName"].ToString().ToLower() == "psmunmanagedsessionaccounts")
                            {
                                accountsDataTable.Rows[i]["Managed"] = "Ad-Hoc Account";
                            }
                        }
                        if (accountsDataTable.Rows[i]["Managed"].ToString() == "")
                        {
                            accountsDataTable.Rows[i]["Managed"] = "n/a";
                            accountsDataTable.Rows[i]["Compliant"] = "n/a";
                        }

                 
                            if (accountsDataTable.Rows[i]["LastSuccessVerification"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["LastSuccessVerification"] = UnixTimeStampToDateTime(accountsDataTable.Rows[i]["LastSuccessVerification"].ToString());
                                accountsDataTable.Rows[i]["Temp2"] = accountsDataTable.Rows[i]["LastSuccessVerification"];

                            }
                            if (accountsDataTable.Rows[i]["LastSuccessReconciliation"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["LastSuccessReconciliation"] = UnixTimeStampToDateTime(accountsDataTable.Rows[i]["LastSuccessReconciliation"].ToString());
                                accountsDataTable.Rows[i]["Temp8"] = accountsDataTable.Rows[i]["LastSuccessReconciliation"];

                            }
                            if (accountsDataTable.Rows[i]["LastSuccessChange"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["LastSuccessChange"] = UnixTimeStampToDateTime(accountsDataTable.Rows[i]["LastSuccessChange"].ToString());
                                accountsDataTable.Rows[i]["Temp1"] = accountsDataTable.Rows[i]["LastSuccessChange"];

                            }
                            if (accountsDataTable.Rows[i]["LastFailDate"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["LastFailDate"] = UnixTimeStampToDateTime(accountsDataTable.Rows[i]["LastFailDate"].ToString());
                                accountsDataTable.Rows[i]["Temp3"] = accountsDataTable.Rows[i]["LastFailDate"];
                            }
                            if (accountsDataTable.Rows[i]["LastModifiedDate"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["Temp5"] = accountsDataTable.Rows[i]["LastModifiedDate"];
                            }
                            if (accountsDataTable.Rows[i]["LastUsedHumanDate"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["Temp6"] = accountsDataTable.Rows[i]["LastUsedHumanDate"];
                            }
                            if (accountsDataTable.Rows[i]["LastUsedComponentDate"].ToString() != "")
                            {
                                accountsDataTable.Rows[i]["Temp7"] = accountsDataTable.Rows[i]["LastUsedComponentDate"];
                            }

                        


                        int changeDaysAgo = 0;
                        int reconciliationDaysAgo = 0;
                        bool changed = false;
                        bool reconciled = false;

                        if (accountsDataTable.Rows[i]["LastSuccessVerification"].ToString() != "")
                        {
                            accountsDataTable.Rows[i]["Verify days ago"] = (int)Math.Floor((complianceDate - Convert.ToDateTime(accountsDataTable.Rows[i]["LastSuccessVerification"].ToString()).Date).TotalDays);
                        }

                        if (accountsDataTable.Rows[i]["LastSuccessChange"].ToString() != "")
                        {
                            changed = true;
                            changeDaysAgo = (int)Math.Floor((complianceDate - Convert.ToDateTime(accountsDataTable.Rows[i]["LastSuccessChange"].ToString()).Date).TotalDays);
                            accountsDataTable.Rows[i]["Change days ago"] = changeDaysAgo;
                        }
                        if (accountsDataTable.Rows[i]["LastSuccessReconciliation"].ToString() != "")
                        {
                            reconciled = true;
                            reconciliationDaysAgo = (int)Math.Floor((complianceDate - Convert.ToDateTime(accountsDataTable.Rows[i]["LastSuccessReconciliation"].ToString()).Date).TotalDays);
                            accountsDataTable.Rows[i]["Reconcile days ago"] = reconciliationDaysAgo;
                        }

                        if (accountsDataTable.Rows[i]["Managed"].ToString() == "Yes")
                        {
                            if (accountsDataTable.Rows[i]["CPMStatus"].ToString() == "success")
                            {
                                managedSuccess += 1;
                            }
                            if (accountsDataTable.Rows[i]["CPMStatus"].ToString() == "failure")
                            {
                                managedFailure += 1;
                            }
                            if (accountsDataTable.Rows[i]["DisabledBy"].ToString() == "CPM")
                            {
                                managedCPMdisabled += 1;
                            }
                            managedAccounts += 1;
             
             
                                accountsDataTable.Rows[i]["Compliant"] = "No";

                                if (changed)
                                {
                                    if (changeDaysAgo <= (int)accountsDataTable.Rows[i]["Rotation interval"])
                                    {
                                        accountsDataTable.Rows[i]["Compliant"] = "Yes";
                                        accountsDataTable.Rows[i]["Days non-compliant"] = 0;
                                    }
                                }
                                if (reconciled)
                                {
                                    if (reconciliationDaysAgo <= (int)accountsDataTable.Rows[i]["Rotation interval"])
                                    {
                                        accountsDataTable.Rows[i]["Compliant"] = "Yes";
                                        accountsDataTable.Rows[i]["Days non-compliant"] = 0;
                                    }
                                }
                                if (!changed && !reconciled)
                                {
                                    changeDaysAgo = (int)Math.Floor((complianceDate - Convert.ToDateTime(accountsDataTable.Rows[i]["CreationDate"].ToString()).Date).TotalDays);
                                    if (changeDaysAgo <= (int)accountsDataTable.Rows[i]["Rotation interval"])
                                    {
                                        accountsDataTable.Rows[i]["Compliant"] = "Yes";
                                        accountsDataTable.Rows[i]["Days non-compliant"] = 0;
                                    }
                                    else
                                    {
                                        accountsDataTable.Rows[i]["Days non-compliant"] = changeDaysAgo - (int)accountsDataTable.Rows[i]["Rotation interval"];
                                    }
                                }
                                if (accountsDataTable.Rows[i]["Compliant"].ToString() == "No")
                                {
                                    if (changed && reconciled)
                                    {
                                        if (changeDaysAgo <= reconciliationDaysAgo)
                                        {
                                            accountsDataTable.Rows[i]["Days non-compliant"] = changeDaysAgo - (int)accountsDataTable.Rows[i]["Rotation interval"];
                                        }
                                        else
                                        {
                                            accountsDataTable.Rows[i]["Days non-compliant"] = reconciliationDaysAgo - (int)accountsDataTable.Rows[i]["Rotation interval"];
                                        }
                                    }
                                    else if (!reconciled && changed)
                                    {
                                        accountsDataTable.Rows[i]["Days non-compliant"] = changeDaysAgo - (int)accountsDataTable.Rows[i]["Rotation interval"];
                                    }
                                    else if (reconciled && !changed)
                                    {
                                        accountsDataTable.Rows[i]["Days non-compliant"] = reconciliationDaysAgo - (int)accountsDataTable.Rows[i]["Rotation interval"];
                                    }
                                }
                  
                        }
                        if (accountsDataTable.Rows[i]["Managed"].ToString() == "Yes")
                        {
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["Managed"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["Managed"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"] + 1;
                                    break;
                                }
                            }
                        }
                        else if (accountsDataTable.Rows[i]["Managed"].ToString() == "No")
                        {
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["Unmanaged"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["Unmanaged"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Unmanaged"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Unmanaged"] + 1;
                                    break;
                                }
                            }
                        }
                        if (accountsDataTable.Rows[i]["Compliant"].ToString() == "Yes")
                        {
                            compliantAccounts += 1;
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["Compliant"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["Compliant"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant"] + 1;
                                    break;
                                }
                            }
                        }
                        else if (accountsDataTable.Rows[i]["Compliant"].ToString() == "No")
                        {
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["Non-Compliant"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["Non-Compliant"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Non-Compliant"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Non-Compliant"] + 1;
                                    break;
                                }
                            }

                            if (accountsDataTable.Rows[i]["CPMStatus"].ToString() == "success")
                            {
                                nonCompliantSuccess += 1;
                            }
                            else if (accountsDataTable.Rows[i]["CPMStatus"].ToString() == "failure")
                            {
                                nonCompliantFailure += 1;
                            }
                            if (accountsDataTable.Rows[i]["DisabledBy"].ToString() == "CPM")
                            {
                                nonCompliantCPMdisabled += 1;
                            }

                        }
                        if (accountsDataTable.Rows[i]["Managed"].ToString() == "n/a")
                        {
                            managedAccountsNA += 1;
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["n/a - missing platform information"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["n/a - missing platform information"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["n/a - missing platform information"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["n/a - missing platform information"] + 1;
                                    break;
                                }
                            }
                        }
                        else if (accountsDataTable.Rows[i]["Managed"].ToString() == "No")
                        {
                            unManagedAccounts += 1;
                        }
                        if (accountsDataTable.Rows[i]["Compliant"].ToString() == "n/a")
                        {
                            for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["PolicyID"].ToString() == managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"].ToString())
                                {
                                    managedAccountsByPolicySortedByManaged.Rows[j]["Compliant n/a"] = (int)managedAccountsByPolicySortedByManaged.Rows[j]["Compliant n/a"] + 1;
                                    break;
                                }
                            }
                            for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                            {
                                if (accountsDataTable.Rows[i]["DeviceType"].ToString() == managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"].ToString())
                                {
                                    managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant n/a"] = (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant n/a"] + 1;
                                    break;
                                }
                            }
                        }
                        else if (accountsDataTable.Rows[i]["Compliant"].ToString() == "No")
                        {
                            nonCompliantAccounts += 1;
                        }
                    }

                    managedAccountsCPMstatus.Columns.Add("Status", typeof(string));
                    managedAccountsCPMstatus.Columns.Add("Managed accounts", typeof(int));
                    managedAccountsCPMstatus.Columns.Add("Share (%)", typeof(double));
                    managedAccountsCPMstatus.Rows.Add("Success", managedSuccess, Math.Round(managedSuccess * 100.00 / managedAccounts, 2));
                    managedAccountsCPMstatus.Rows.Add("Failure", managedFailure, Math.Round(managedFailure * 100.00 / managedAccounts, 2));
                    managedAccountsCPMstatus.Rows.Add("Disabled by CPM", managedCPMdisabled, Math.Round(managedCPMdisabled * 100.00 / managedAccounts, 2));
                    managedAccountsCPMstatusShow = managedAccountsCPMstatus.Copy();
                    managedAccountsCPMstatus.Rows.Add("Not disabled by CPM", managedAccounts - managedCPMdisabled, 100 - Math.Round(managedCPMdisabled * 100.00 / managedAccounts, 2));


                    nonCompliantAccountsCPMstatus.Columns.Add("Status", typeof(string));
                    nonCompliantAccountsCPMstatus.Columns.Add("Non-compliant accounts", typeof(int));
                    nonCompliantAccountsCPMstatus.Columns.Add("Share (%)", typeof(double));
                    nonCompliantAccountsCPMstatus.Rows.Add("Success", nonCompliantSuccess, Math.Round(nonCompliantSuccess * 100.00 / nonCompliantAccounts, 2));
                    nonCompliantAccountsCPMstatus.Rows.Add("Failure", nonCompliantFailure, Math.Round(nonCompliantFailure * 100.00 / nonCompliantAccounts, 2));
                    nonCompliantAccountsCPMstatus.Rows.Add("Disabled by CPM", nonCompliantCPMdisabled, Math.Round(nonCompliantCPMdisabled * 100.00 / nonCompliantAccounts, 2));
                    nonCompliantAccountsCPMstatusShow = nonCompliantAccountsCPMstatus.Copy();
                    nonCompliantAccountsCPMstatus.Rows.Add("Not disabled by CPM", nonCompliantAccounts - nonCompliantCPMdisabled, Math.Round(100 - nonCompliantCPMdisabled * 100.00 / nonCompliantAccounts, 2));

                    notManagedReasonsForAccounts.Columns.Add("Not managed reason", typeof(string));
                    notManagedReasonsForAccounts.Columns.Add("Number of accounts", typeof(int));
                    if (unManagedAccounts > 0)
                    {
                        int noPolicy = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Managed") == "No" && p.Field<string>("PolicyID") is null select p.Field<string>("Managed")).Count();
                        if (noPolicy > 0)
                        {
                            notManagedReasonsForAccounts.Rows.Add("Not assigned to a policy", noPolicy);
                        }
                        int noPeriodicChange = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Managed") == "No" && p.Field<string>("PeriodicChange") == "No" select p.Field<string>("Managed")).Count();
                        if (noPeriodicChange > 0)
                        {
                            notManagedReasonsForAccounts.Rows.Add("Periodic change disabled", noPeriodicChange);
                        }
                        int manuallyDisabled = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Managed") == "No" && p.Field<string>("DisabledBy") == "User" select p.Field<string>("Managed")).Count();
                        if (manuallyDisabled > 0)
                        {
                            notManagedReasonsForAccounts.Rows.Add("Disabled by user", manuallyDisabled);
                        }
                        int notOwnedByCPMs = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Managed") == "No" && p.Field<string>("Owned by CPM(s)") == "No" select p.Field<string>("Managed")).Count();
                        if (notOwnedByCPMs > 0)
                        {
                            notManagedReasonsForAccounts.Rows.Add("Not owned by enabled CPM", notOwnedByCPMs);
                        }
                        int notAllowedForCPMs = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Managed") == "No" && p.Field<string>("Allowed for CPM(s)") == "No" select p.Field<string>("Managed")).Count();
                        if (notAllowedForCPMs > 0)
                        {
                            notManagedReasonsForAccounts.Rows.Add("Not allowed for CPM(s)", notAllowedForCPMs);
                        }
                    }

                    notManagedReasonsForAccounts.DefaultView.Sort = "Number of accounts DESC";
                    notManagedReasonsForAccounts = notManagedReasonsForAccounts.DefaultView.ToTable();

                    // Determining accounts CPM status...


                    for (int j = 0; j < managedAccountsByPolicySortedByManaged.Rows.Count; j++)
                    {
                        managedAccountsByPolicySortedByManaged.Rows[j]["Managed (%)"] = Math.Round((int)managedAccountsByPolicySortedByManaged.Rows[j]["Managed"] * 100.00 / (int)managedAccountsByPolicySortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                        managedAccountsByPolicySortedByManaged.Rows[j]["Unmanaged (%)"] = Math.Round((int)managedAccountsByPolicySortedByManaged.Rows[j]["Unmanaged"] * 100.00 / (int)managedAccountsByPolicySortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                        managedAccountsByPolicySortedByManaged.Rows[j]["n/a - missing platform information (%)"] = Math.Round((int)managedAccountsByPolicySortedByManaged.Rows[j]["n/a - missing platform information"] * 100.00 / (int)managedAccountsByPolicySortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                        if ((int)managedAccountsByPolicySortedByManaged.Rows[j]["Managed"] > 0)
                        {
                            managedPolicies += 1;
                            managedAccountsByPolicySortedByManaged.Rows[j]["Compliant (%)"] = Math.Round((int)managedAccountsByPolicySortedByManaged.Rows[j]["Compliant"] * 100.00 / (int)managedAccountsByPolicySortedByManaged.Rows[j]["Managed"], 2);
                            managedAccountsByPolicySortedByManaged.Rows[j]["Non-Compliant (%)"] = Math.Round((int)managedAccountsByPolicySortedByManaged.Rows[j]["Non-Compliant"] * 100.00 / (int)managedAccountsByPolicySortedByManaged.Rows[j]["Managed"], 2);
                        }
                        if ((int)managedAccountsByPolicySortedByManaged.Rows[j]["Non-Compliant"] > 0)
                        {
                            managedAccountsByPolicySortedByManaged.Rows[j]["Average days non-compliant"] = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Compliant") == "No" && p.Field<string>("PolicyID") == (string)managedAccountsByPolicySortedByManaged.Rows[j]["PolicyID"] select p.Field<int>("Days non-compliant")).Average();
                        }
                        else
                        {
                            managedAccountsByPolicySortedByManaged.Rows[j]["Average days non-compliant"] = 0;
                        }
                    }


            
                        for (int j = 0; j < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; j++)
                        {
                            managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                            managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Unmanaged (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Unmanaged"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                            managedAccountsByDeviceTypeSortedByManaged.Rows[j]["n/a - missing platform information (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["n/a - missing platform information"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["NumberOfAccounts"], 2);
                            if ((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"] > 0)
                            {
                                managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"], 2);
                                managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Non-Compliant (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Non-Compliant"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"], 2);
                                managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant n/a (%)"] = Math.Round((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Compliant n/a"] * 100.00 / (int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Managed"], 2);
                            }
                            if ((int)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Non-Compliant"] > 0)
                            {
                                managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Average days non-compliant"] = (from p in accountsDataTable.AsEnumerable() where p.Field<string>("Compliant") == "No" && p.Field<string>("DeviceType") == (string)managedAccountsByDeviceTypeSortedByManaged.Rows[j]["DeviceType"] select p.Field<int>("Days non-compliant")).Average();
                            }
                            else
                            {
                                managedAccountsByDeviceTypeSortedByManaged.Rows[j]["Average days non-compliant"] = 0;
                            }
                        }

                    averageDaysNonCompliantbyDeviceType.Columns.Add("DeviceType", typeof(string));
                    averageDaysNonCompliantbyDeviceType.Columns.Add("Accounts", typeof(int));
                    averageDaysNonCompliantbyDeviceType.Columns.Add("Non-Compliant", typeof(int));
                    averageDaysNonCompliantbyDeviceType.Columns.Add("Non-Compliant (%)", typeof(double));
                    averageDaysNonCompliantbyDeviceType.Columns.Add("Average days non-compliant", typeof(int));

                    for (int i = 0; i < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; i++)
                    {
                        if ((int)managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant"] > 0)
                        {
                            averageDaysNonCompliantbyDeviceType.Rows.Add(managedAccountsByDeviceTypeSortedByManaged.Rows[i]["DeviceType"], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["NumberOfAccounts"], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant"], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant (%)"], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Average days non-compliant"]);
                        }
                    }


                    averageDaysNonCompliantbyDeviceType.DefaultView.Sort = "Average days non-compliant DESC";
                    averageDaysNonCompliantbyDeviceType = averageDaysNonCompliantbyDeviceType.DefaultView.ToTable();

                    averageDaysNonCompliantbyPolicy.Columns.Add("PolicyID", typeof(string));
                    averageDaysNonCompliantbyPolicy.Columns.Add("Accounts", typeof(int));
                    averageDaysNonCompliantbyPolicy.Columns.Add("Non-Compliant", typeof(int));
                    averageDaysNonCompliantbyPolicy.Columns.Add("Non-Compliant (%)", typeof(double));
                    averageDaysNonCompliantbyPolicy.Columns.Add("Average days non-compliant", typeof(int));


                    for (int i = 0; i < managedAccountsByPolicySortedByManaged.Rows.Count; i++)
                    {
                        if ((int)managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant"] > 0)
                        {
                            averageDaysNonCompliantbyPolicy.Rows.Add(managedAccountsByPolicySortedByManaged.Rows[i]["PolicyID"], managedAccountsByPolicySortedByManaged.Rows[i]["NumberOfAccounts"], managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant"], managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant (%)"], managedAccountsByPolicySortedByManaged.Rows[i]["Average days non-compliant"]);
                        }
                    }
                    averageDaysNonCompliantbyPolicy.DefaultView.Sort = "Average days non-compliant DESC";
                    averageDaysNonCompliantbyPolicy = averageDaysNonCompliantbyPolicy.DefaultView.ToTable();


                        accountsDataTable.Columns.Remove("LastSuccessVerification");
                        accountsDataTable.Columns.Remove("LastSuccessReconciliation");
                        accountsDataTable.Columns.Remove("LastSuccessChange");
                        accountsDataTable.Columns.Remove("LastFailDate");
                        accountsDataTable.Columns.Remove("DeletionDate");
                        accountsDataTable.Columns.Remove("LastModifiedDate");
                        accountsDataTable.Columns.Remove("LastUsedHumanDate");
                        accountsDataTable.Columns.Remove("LastUsedComponentDate");

                        accountsDataTable.Columns["Temp3"].ColumnName = "LastFailDate";
                        accountsDataTable.Columns["LastFailDate"].SetOrdinal(accountsDataTable.Columns["CPMErrorDetails"].Ordinal + 1);
                        accountsDataTable.Columns["Temp1"].ColumnName = "LastSuccessChange";
                        accountsDataTable.Columns["LastSuccessChange"].SetOrdinal(accountsDataTable.Columns["LastFailDate"].Ordinal + 1);
                        accountsDataTable.Columns["Temp8"].ColumnName = "LastSuccessReconciliation";
                        accountsDataTable.Columns["LastSuccessReconciliation"].SetOrdinal(accountsDataTable.Columns["LastSuccessChange"].Ordinal + 1);
                        accountsDataTable.Columns["Temp2"].ColumnName = "LastSuccessVerification";
                        accountsDataTable.Columns["LastSuccessVerification"].SetOrdinal(accountsDataTable.Columns["LastSuccessReconciliation"].Ordinal + 1);
                        accountsDataTable.Columns["Verify days ago"].SetOrdinal(accountsDataTable.Columns["LastSuccessVerification"].Ordinal + 1);
                        accountsDataTable.Columns["Temp4"].ColumnName = "DeletionDate";
                        accountsDataTable.Columns["DeletionDate"].SetOrdinal(accountsDataTable.Columns["DeletedBy"].Ordinal + 1);
                        accountsDataTable.Columns["Temp5"].ColumnName = "LastModifiedDate";
                        accountsDataTable.Columns["LastModifiedDate"].SetOrdinal(accountsDataTable.Columns["LastModifiedBy"].Ordinal + 1);
                        accountsDataTable.Columns["Temp6"].ColumnName = "LastUsedHumanDate";
                        accountsDataTable.Columns["LastUsedHumanDate"].SetOrdinal(accountsDataTable.Columns["LastUsedByHuman"].Ordinal + 1);
                        accountsDataTable.Columns["Temp7"].ColumnName = "LastUsedComponentDate";
                        accountsDataTable.Columns["LastUsedComponentDate"].SetOrdinal(accountsDataTable.Columns["LastUsedByComponent"].Ordinal + 1);
                        accountsDataTable.Columns["CPMStatus"].SetOrdinal(accountsDataTable.Columns["Days non-compliant"].Ordinal + 1);
                        accountsDataTable.Columns["CPMErrorDetails"].SetOrdinal(accountsDataTable.Columns["CPMStatus"].Ordinal + 1);



                    DataTable ManagedAccountsCPMstatusSuccess = new DataTable();
                    DataTable ManagedAccountsCPMstatusFailure = new DataTable();
                    DataTable ManagedAccountsCPMstatusDisabledByCPM = new DataTable();
                    ManagedAccountsCPMstatusSuccess = (from p in accountsDataTable.AsEnumerable()
                                                       where p.Field<string>("Managed") == "Yes" && p.Field<string>("CPMStatus") == "success"
                                                       group p by p.Field<string>("PolicyID") into d
                                                       select new
                                                       {
                                                           PolicyID = d.Key,
                                                           Success = d.Count(),
                                                       }).ToDataTable();

                    ManagedAccountsCPMstatusFailure = (from p in accountsDataTable.AsEnumerable()
                                                       where p.Field<string>("Managed") == "Yes" && p.Field<string>("CPMStatus") == "failure"
                                                       group p by p.Field<string>("PolicyID") into d
                                                       select new
                                                       {
                                                           PolicyID = d.Key,
                                                           Failure = d.Count(),
                                                       }).ToDataTable();

                    ManagedAccountsCPMstatusDisabledByCPM = (from p in accountsDataTable.AsEnumerable()
                                                             where p.Field<string>("Managed") == "Yes" && p.Field<string>("DisabledBy") == "CPM"
                                                             group p by p.Field<string>("PolicyID") into d
                                                             select new
                                                             {
                                                                 PolicyID = d.Key,
                                                                 DisabledByCPM = d.Count(),
                                                             }).ToDataTable();


                    managedAccountsCPMstatusByPolicy.Columns.Add("PolicyID", typeof(string));
                    managedAccountsCPMstatusByPolicy.Columns.Add("ManagedAccounts", typeof(int));
                    managedAccountsCPMstatusByPolicy.Columns.Add("Success", typeof(int));
                    managedAccountsCPMstatusByPolicy.Columns.Add("Failure", typeof(int));
                    managedAccountsCPMstatusByPolicy.Columns.Add("DisabledByCPM", typeof(int));
                    managedAccountsCPMstatusByPolicy.Columns.Add("Success (%)", typeof(double));
                    managedAccountsCPMstatusByPolicy.Columns.Add("Failure (%)", typeof(double));
                    managedAccountsCPMstatusByPolicy.Columns.Add("DisabledByCPM (%)", typeof(double));

                    for (int i = 0; i < managedAccountsByPolicySortedByManaged.Rows.Count; i++)
                    {
                        if (Convert.ToInt32(managedAccountsByPolicySortedByManaged.Rows[i]["Managed"]) > 0)
                        {
                            managedAccountsCPMstatusByPolicy.Rows.Add(managedAccountsByPolicySortedByManaged.Rows[i][0], managedAccountsByPolicySortedByManaged.Rows[i]["Managed"], 0, 0, 0);
                        }
                    }


                    for (int i = 0; i < managedAccountsCPMstatusByPolicy.Rows.Count; i++)
                    {
                        for (int j = 0; j < ManagedAccountsCPMstatusSuccess.Rows.Count; j++)
                        {

                            if (managedAccountsCPMstatusByPolicy.Rows[i][0].ToString() == ManagedAccountsCPMstatusSuccess.Rows[j][0].ToString())
                            {
                                managedAccountsCPMstatusByPolicy.Rows[i][2] = ManagedAccountsCPMstatusSuccess.Rows[j][1];
                            }
                        }
                        for (int k = 0; k < ManagedAccountsCPMstatusFailure.Rows.Count; k++)
                        {
                            if (managedAccountsCPMstatusByPolicy.Rows[i][0].ToString() == ManagedAccountsCPMstatusFailure.Rows[k][0].ToString())
                            {
                                managedAccountsCPMstatusByPolicy.Rows[i][3] = ManagedAccountsCPMstatusFailure.Rows[k][1];
                            }
                        }
                        for (int l = 0; l < ManagedAccountsCPMstatusDisabledByCPM.Rows.Count; l++)
                        {
                            if (managedAccountsCPMstatusByPolicy.Rows[i][0].ToString() == ManagedAccountsCPMstatusDisabledByCPM.Rows[l][0].ToString())
                            {
                                managedAccountsCPMstatusByPolicy.Rows[i][4] = ManagedAccountsCPMstatusDisabledByCPM.Rows[l][1];
                            }
                        }
                        managedAccountsCPMstatusByPolicy.Rows[i][5] = Math.Round((int)managedAccountsCPMstatusByPolicy.Rows[i][2] * 100.00 / (int)managedAccountsCPMstatusByPolicy.Rows[i][1], 2);
                        managedAccountsCPMstatusByPolicy.Rows[i][6] = Math.Round((int)managedAccountsCPMstatusByPolicy.Rows[i][3] * 100.00 / (int)managedAccountsCPMstatusByPolicy.Rows[i][1], 2);
                        managedAccountsCPMstatusByPolicy.Rows[i][7] = Math.Round((int)managedAccountsCPMstatusByPolicy.Rows[i][4] * 100.00 / (int)managedAccountsCPMstatusByPolicy.Rows[i][1], 2);
                    }

                    managedAccountsCPMstatusByPolicy.DefaultView.Sort = "Failure (%) DESC";
                    managedAccountsCPMstatusByPolicy = managedAccountsCPMstatusByPolicy.DefaultView.ToTable();


                    DataTable ManagedAccountsCPMstatusSuccessByDeviceType = new DataTable();
                    DataTable ManagedAccountsCPMstatusFailureByDeviceType = new DataTable();
                    DataTable ManagedAccountsCPMstatusDisabledByCPMByDeviceType = new DataTable();

                    managedAccountsCPMstatusByDeviceType.Columns.Add("DeviceType", typeof(string));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("ManagedAccounts", typeof(int));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("Success", typeof(int));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("Failure", typeof(int));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("DisabledByCPM", typeof(int));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("Success (%)", typeof(double));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("Failure (%)", typeof(double));
                    managedAccountsCPMstatusByDeviceType.Columns.Add("DisabledByCPM (%)", typeof(double));

                    ManagedAccountsCPMstatusSuccessByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                   where p.Field<string>("Managed") == "Yes" && p.Field<string>("CPMStatus") == "success"
                                                                   group p by p.Field<string>("DeviceType") into d
                                                                   select new
                                                                   {
                                                                       DeviceType = d.Key,
                                                                       Success = d.Count(),
                                                                   }).ToDataTable();

                    ManagedAccountsCPMstatusFailureByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                   where p.Field<string>("Managed") == "Yes" && p.Field<string>("CPMStatus") == "failure"
                                                                   group p by p.Field<string>("DeviceType") into d
                                                                   select new
                                                                   {
                                                                       DeviceType = d.Key,
                                                                       Failure = d.Count(),
                                                                   }).ToDataTable();

                    ManagedAccountsCPMstatusDisabledByCPMByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                         where p.Field<string>("Managed") == "Yes" && p.Field<string>("DisabledBy") == "CPM"
                                                                         group p by p.Field<string>("DeviceType") into d
                                                                         select new
                                                                         {
                                                                             DeviceType = d.Key,
                                                                             DisabledByCPM = d.Count(),
                                                                         }).ToDataTable();



                    for (int i = 0; i < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; i++)
                    {
                        if (Convert.ToInt32(managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Managed"]) > 0)
                        {
                            managedAccountsCPMstatusByDeviceType.Rows.Add(managedAccountsByDeviceTypeSortedByManaged.Rows[i][0], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Managed"], 0, 0, 0);
                        }

                    }



                    for (int i = 0; i < managedAccountsCPMstatusByDeviceType.Rows.Count; i++)
                    {
                        for (int j = 0; j < ManagedAccountsCPMstatusSuccessByDeviceType.Rows.Count; j++)
                        {

                            if (managedAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == ManagedAccountsCPMstatusSuccessByDeviceType.Rows[j][0].ToString())
                            {
                                managedAccountsCPMstatusByDeviceType.Rows[i][2] = ManagedAccountsCPMstatusSuccessByDeviceType.Rows[j][1];
                            }
                        }
                        for (int k = 0; k < ManagedAccountsCPMstatusFailureByDeviceType.Rows.Count; k++)
                        {
                            if (managedAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == ManagedAccountsCPMstatusFailureByDeviceType.Rows[k][0].ToString())
                            {
                                managedAccountsCPMstatusByDeviceType.Rows[i][3] = ManagedAccountsCPMstatusFailureByDeviceType.Rows[k][1];
                            }
                        }
                        for (int l = 0; l < ManagedAccountsCPMstatusDisabledByCPMByDeviceType.Rows.Count; l++)
                        {
                            if (managedAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == ManagedAccountsCPMstatusDisabledByCPMByDeviceType.Rows[l][0].ToString())
                            {
                                managedAccountsCPMstatusByDeviceType.Rows[i][4] = ManagedAccountsCPMstatusDisabledByCPMByDeviceType.Rows[l][1];
                            }
                        }
                        managedAccountsCPMstatusByDeviceType.Rows[i][5] = Math.Round((int)managedAccountsCPMstatusByDeviceType.Rows[i][2] * 100.00 / (int)managedAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                        managedAccountsCPMstatusByDeviceType.Rows[i][6] = Math.Round((int)managedAccountsCPMstatusByDeviceType.Rows[i][3] * 100.00 / (int)managedAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                        managedAccountsCPMstatusByDeviceType.Rows[i][7] = Math.Round((int)managedAccountsCPMstatusByDeviceType.Rows[i][4] * 100.00 / (int)managedAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                    }


                    managedAccountsCPMstatusByDeviceType.DefaultView.Sort = "Failure (%) DESC";
                    managedAccountsCPMstatusByDeviceType = managedAccountsCPMstatusByDeviceType.DefaultView.ToTable();


                    DataTable NonCompliantAccountsCPMstatusSuccessByPolicy = new DataTable();
                    DataTable NonCompliantAccountsCPMstatusFailureByPolicy = new DataTable();
                    DataTable NonCompliantAccountsCPMstatusDisabledByCPMByPolicy = new DataTable();

                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("PolicyID", typeof(string));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("Non-CompliantAccounts", typeof(int));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("Success", typeof(int));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("Failure", typeof(int));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("DisabledByCPM", typeof(int));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("Success (%)", typeof(double));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("Failure (%)", typeof(double));
                    nonCompliantAccountsCPMstatusByPolicy.Columns.Add("DisabledByCPM (%)", typeof(double));

                    for (int i = 0; i < managedAccountsByPolicySortedByManaged.Rows.Count; i++)
                    {
                        if (Convert.ToInt32(managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant"]) > 0)
                        {
                            nonCompliantAccountsCPMstatusByPolicy.Rows.Add(managedAccountsByPolicySortedByManaged.Rows[i][0], managedAccountsByPolicySortedByManaged.Rows[i]["Non-Compliant"], 0, 0, 0);
                        }
                    }


                    NonCompliantAccountsCPMstatusSuccessByPolicy = (from p in accountsDataTable.AsEnumerable()
                                                                    where p.Field<string>("Compliant") == "No" && p.Field<string>("CPMStatus") == "success"
                                                                    group p by p.Field<string>("PolicyID") into d
                                                                    select new
                                                                    {
                                                                        PolicyID = d.Key,
                                                                        Success = d.Count(),
                                                                    }).ToDataTable();

                    NonCompliantAccountsCPMstatusFailureByPolicy = (from p in accountsDataTable.AsEnumerable()
                                                                    where p.Field<string>("Compliant") == "No" && p.Field<string>("CPMStatus") == "failure"
                                                                    group p by p.Field<string>("PolicyID") into d
                                                                    select new
                                                                    {
                                                                        PolicyID = d.Key,
                                                                        Failure = d.Count(),
                                                                    }).ToDataTable();

                    NonCompliantAccountsCPMstatusDisabledByCPMByPolicy = (from p in accountsDataTable.AsEnumerable()
                                                                          where p.Field<string>("Compliant") == "No" && p.Field<string>("DisabledBy") == "CPM"
                                                                          group p by p.Field<string>("PolicyID") into d
                                                                          select new
                                                                          {
                                                                              PolicyID = d.Key,
                                                                              DisabledByCPM = d.Count(),
                                                                          }).ToDataTable();


                    for (int i = 0; i < nonCompliantAccountsCPMstatusByPolicy.Rows.Count; i++)
                    {
                        for (int j = 0; j < NonCompliantAccountsCPMstatusSuccessByPolicy.Rows.Count; j++)
                        {

                            if (nonCompliantAccountsCPMstatusByPolicy.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusSuccessByPolicy.Rows[j][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByPolicy.Rows[i][2] = NonCompliantAccountsCPMstatusSuccessByPolicy.Rows[j][1];
                            }
                        }
                        for (int k = 0; k < NonCompliantAccountsCPMstatusFailureByPolicy.Rows.Count; k++)
                        {
                            if (nonCompliantAccountsCPMstatusByPolicy.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusFailureByPolicy.Rows[k][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByPolicy.Rows[i][3] = NonCompliantAccountsCPMstatusFailureByPolicy.Rows[k][1];
                            }
                        }
                        for (int l = 0; l < NonCompliantAccountsCPMstatusDisabledByCPMByPolicy.Rows.Count; l++)
                        {
                            if (nonCompliantAccountsCPMstatusByPolicy.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusDisabledByCPMByPolicy.Rows[l][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByPolicy.Rows[i][4] = NonCompliantAccountsCPMstatusDisabledByCPMByPolicy.Rows[l][1];
                            }
                        }
                        nonCompliantAccountsCPMstatusByPolicy.Rows[i][5] = Math.Round((int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][2] * 100.00 / (int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][1], 2);
                        nonCompliantAccountsCPMstatusByPolicy.Rows[i][6] = Math.Round((int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][3] * 100.00 / (int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][1], 2);
                        nonCompliantAccountsCPMstatusByPolicy.Rows[i][7] = Math.Round((int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][4] * 100.00 / (int)nonCompliantAccountsCPMstatusByPolicy.Rows[i][1], 2);
                    }


                    nonCompliantAccountsCPMstatusByPolicy.DefaultView.Sort = "Failure (%) DESC";
                    nonCompliantAccountsCPMstatusByPolicy = nonCompliantAccountsCPMstatusByPolicy.DefaultView.ToTable();


                    DataTable NonCompliantAccountsCPMstatusSuccessByDeviceType = new DataTable();
                    DataTable NonCompliantAccountsCPMstatusFailureByDeviceType = new DataTable();
                    DataTable NonCompliantAccountsCPMstatusDisabledByCPMByDeviceType = new DataTable();

                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("DeviceType", typeof(string));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("Non-CompliantAccounts", typeof(int));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("Success", typeof(int));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("Failure", typeof(int));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("DisabledByCPM", typeof(int));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("Success (%)", typeof(double));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("Failure (%)", typeof(double));
                    nonCompliantAccountsCPMstatusByDeviceType.Columns.Add("DisabledByCPM (%)", typeof(double));


                    for (int i = 0; i < managedAccountsByDeviceTypeSortedByManaged.Rows.Count; i++)
                    {
                        if (Convert.ToInt32(managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant"]) > 0)
                        {
                            nonCompliantAccountsCPMstatusByDeviceType.Rows.Add(managedAccountsByDeviceTypeSortedByManaged.Rows[i][0], managedAccountsByDeviceTypeSortedByManaged.Rows[i]["Non-Compliant"], 0, 0, 0);
                        }
                    }

                    NonCompliantAccountsCPMstatusSuccessByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                        where p.Field<string>("Compliant") == "No" && p.Field<string>("CPMStatus") == "success"
                                                                        group p by p.Field<string>("DeviceType") into d
                                                                        select new
                                                                        {
                                                                            DeviceType = d.Key,
                                                                            Success = d.Count(),
                                                                        }).ToDataTable();

                    NonCompliantAccountsCPMstatusFailureByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                        where p.Field<string>("Compliant") == "No" && p.Field<string>("CPMStatus") == "failure"
                                                                        group p by p.Field<string>("DeviceType") into d
                                                                        select new
                                                                        {
                                                                            DeviceType = d.Key,
                                                                            Failure = d.Count(),
                                                                        }).ToDataTable();

                    NonCompliantAccountsCPMstatusDisabledByCPMByDeviceType = (from p in accountsDataTable.AsEnumerable()
                                                                              where p.Field<string>("Compliant") == "No" && p.Field<string>("DisabledBy") == "CPM"
                                                                              group p by p.Field<string>("DeviceType") into d
                                                                              select new
                                                                              {
                                                                                  DeviceType = d.Key,
                                                                                  DisabledByCPM = d.Count(),
                                                                              }).ToDataTable();



                    for (int i = 0; i < nonCompliantAccountsCPMstatusByDeviceType.Rows.Count; i++)
                    {
                        for (int j = 0; j < NonCompliantAccountsCPMstatusSuccessByDeviceType.Rows.Count; j++)
                        {

                            if (nonCompliantAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusSuccessByDeviceType.Rows[j][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByDeviceType.Rows[i][2] = NonCompliantAccountsCPMstatusSuccessByDeviceType.Rows[j][1];
                            }
                        }
                        for (int k = 0; k < NonCompliantAccountsCPMstatusFailureByDeviceType.Rows.Count; k++)
                        {
                            if (nonCompliantAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusFailureByDeviceType.Rows[k][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByDeviceType.Rows[i][3] = NonCompliantAccountsCPMstatusFailureByDeviceType.Rows[k][1];
                            }
                        }
                        for (int l = 0; l < NonCompliantAccountsCPMstatusDisabledByCPMByDeviceType.Rows.Count; l++)
                        {
                            if (nonCompliantAccountsCPMstatusByDeviceType.Rows[i][0].ToString() == NonCompliantAccountsCPMstatusDisabledByCPMByDeviceType.Rows[l][0].ToString())
                            {
                                nonCompliantAccountsCPMstatusByDeviceType.Rows[i][4] = NonCompliantAccountsCPMstatusDisabledByCPMByDeviceType.Rows[l][1];
                            }
                        }
                        nonCompliantAccountsCPMstatusByDeviceType.Rows[i][5] = Math.Round((int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][2] * 100.00 / (int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                        nonCompliantAccountsCPMstatusByDeviceType.Rows[i][6] = Math.Round((int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][3] * 100.00 / (int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                        nonCompliantAccountsCPMstatusByDeviceType.Rows[i][7] = Math.Round((int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][4] * 100.00 / (int)nonCompliantAccountsCPMstatusByDeviceType.Rows[i][1], 2);
                    }

                    nonCompliantAccountsCPMstatusByDeviceType.DefaultView.Sort = "Failure (%) DESC";
                    nonCompliantAccountsCPMstatusByDeviceType = nonCompliantAccountsCPMstatusByDeviceType.DefaultView.ToTable();

                    managedAccountsStatisticsDataTable.Columns.Add("Statistic", typeof(string));
                    managedAccountsStatisticsDataTable.Columns.Add("Value", typeof(double));
                    managedAccountsStatisticsDataTable.Rows.Add("Managed accounts", managedAccounts);
                    managedAccountsStatisticsDataTable.Rows.Add("Unmanaged accounts", unManagedAccounts);
                    managedAccountsStatisticsDataTable.Rows.Add("Managed accounts (%)", Math.Round(managedAccounts * 100.00 / effectiveNumberOfAccounts, 2));
                    managedAccountsStatisticsDataTable.Rows.Add("Unmanaged accounts (%)", Math.Round(unManagedAccounts * 100.00 / effectiveNumberOfAccounts, 2));
                    managedAccountsStatisticsDataTable.Rows.Add("Compliant accounts", compliantAccounts);
                    managedAccountsStatisticsDataTable.Rows.Add("Non-compliant accounts", nonCompliantAccounts);
                    managedAccountsStatisticsDataTable.Rows.Add("Compliant accounts (%)", Math.Round(compliantAccounts * 100.00 / managedAccounts, 2));
                    managedAccountsStatisticsDataTable.Rows.Add("Non-compliant accounts (%)", Math.Round(nonCompliantAccounts * 100.00 / managedAccounts, 2));
                    if (managedAccountsNA > 0)
                    {
                        managedAccountsStatisticsDataTable.Rows.Add("n/a - missing platform information", managedAccountsNA);
                        managedAccountsStatisticsDataTable.Rows.Add("n/a - missing platform information (%)", Math.Round(managedAccountsNA * 100.00 / effectiveNumberOfAccounts, 2));
                    }

                    managedAccountsDataTable.Columns.Add("Managed", typeof(string));
                    managedAccountsDataTable.Columns.Add("Value", typeof(double));
                    managedAccountsDataTable.Rows.Add("Managed Accounts", Math.Round(managedAccounts * 100.00 / effectiveNumberOfAccounts, 2));
                    managedAccountsDataTable.Rows.Add("Unmanaged Accounts", Math.Round(unManagedAccounts * 100.00 / effectiveNumberOfAccounts, 2));
                    if (managedAccountsNA > 0)
                    {
                        managedAccountsDataTable.Rows.Add("n/a - missing platform information", Math.Round(managedAccountsNA * 100.00 / effectiveNumberOfAccounts, 2));
                    }


                    managedAccountsByPolicySortedByManaged.DefaultView.Sort = "Managed (%) DESC, Compliant (%) DESC";
                    managedAccountsByPolicySortedByManaged = managedAccountsByPolicySortedByManaged.DefaultView.ToTable();

                    managedAccountsByPolicySortedByCompliance = managedAccountsByPolicySortedByManaged.Copy();
                    managedAccountsByPolicySortedByCompliance.DefaultView.Sort = "Compliant (%) DESC, Managed (%) DESC";
                    managedAccountsByPolicySortedByCompliance = managedAccountsByPolicySortedByCompliance.DefaultView.ToTable();

                    managedAccountsByDeviceTypeSortedByManaged.DefaultView.Sort = "Managed (%) DESC, Compliant (%) DESC";
                    managedAccountsByDeviceTypeSortedByManaged = managedAccountsByDeviceTypeSortedByManaged.DefaultView.ToTable();


                    managedAccountsByDeviceTypeSortedByCompliance = managedAccountsByDeviceTypeSortedByManaged.Copy();
                    managedAccountsByDeviceTypeSortedByCompliance.DefaultView.Sort = "Compliant (%) DESC, Managed (%) DESC";
                    managedAccountsByDeviceTypeSortedByCompliance = managedAccountsByDeviceTypeSortedByCompliance.DefaultView.ToTable();

                    compliantAccountsDataTable.Columns.Add("Compliant", typeof(string));
                    compliantAccountsDataTable.Columns.Add("Value", typeof(double));
                    compliantAccountsDataTable.Columns.Add("Accounts", typeof(int));
                    compliantAccountsDataTable.Rows.Add("Compliant Accounts", Math.Round(compliantAccounts * 100.00 / managedAccounts, 2), compliantAccounts);
                    compliantAccountsDataTable.Rows.Add("Non-Compliant Accounts", Math.Round(nonCompliantAccounts * 100.00 / managedAccounts, 2), nonCompliantAccounts);


                    accountsByCPMStatusDataTable = new DataTable();
                    accountsByCPMStatusDataTable.Columns.Add("CPM Status", typeof(string));
                    accountsByCPMStatusDataTable.Columns.Add("NumberOfAccounts", typeof(int));
                    accountsByCPMStatusDataTable.Columns.Add("Share", typeof(string));
                    queryString =
                        "select objectproperties.ObjectPropertyValue as 'CPM Status', Count(objectproperties.ObjectPropertyValue) as NumberOfAccounts from objectproperties,files where objectproperties.ObjectPropertyName == 'CPMStatus' and files.Type == 2 and files.DeletedBy == '' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID group by objectproperties.ObjectPropertyValue COLLATE NOCASE";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(accountsByCPMStatusDataTable);
                    DBFunctions.closeDBConnection();

                    for (int i = 0; i < accountsByCPMStatusDataTable.Rows.Count; i++)
                    {
                        accountsByCPMStatusDataTable.Rows[i][2] = Math.Round(Int32.Parse(accountsByCPMStatusDataTable.Rows[i][1].ToString()) * 100.00 / accountsByCPMStatusDataTable.AsEnumerable().Sum(x => x.Field<int>("NumberOfAccounts")), 2) + " %";
                    }

                    accountCreatorsDataTable = new DataTable();
                    accountCreatorsDataTable = (from p in files
                                                where (p.Type == 2 && !p.SafeName.ToLower().EndsWith("_workspace") && p.SafeName.ToLower() != "psmunmanagedsessionaccounts")
                                                group p by new { p.CreatedBy } into d
                                                orderby d.Count() descending
                                                select new
                                                {
                                                    UserName = d.Key.CreatedBy,
                                                    CreatedAccounts = d.Count(),
                                                    Share = Math.Round(d.Count() * 100.00 / numberOfAccounts, 2)

                                                }).ToDataTable();

                    accountCreatorsDataTable.Columns["Share"].ColumnName = "Share (%)";

                    accountDeletersDataTable = new DataTable();
                    accountDeletersDataTable = (from p in files
                                                where (p.Type == 2 && p.DeletedBy != "" && !p.SafeName.ToLower().EndsWith("_workspace") && p.SafeName.ToLower() != "psmunmanagedsessionaccounts")
                                                group p by new { p.DeletedBy }
                        into d
                                                orderby d.Count() descending
                                                select new
                                                {
                                                    UserName = d.Key.DeletedBy,
                                                    RemovedAccounts = d.Count(),
                                                    Share = Math.Round(d.Count() * 100.00 / numberOfDeletedAccounts, 2)
                                                }).ToDataTable();

                    accountDeletersDataTable.Columns["Share"].ColumnName = "Share (%)";


                    accountCPMStatusByPolicyDataTable = new DataTable();
                    accountCPMStatusByPolicyDataTable.Columns.Add("PolicyID", typeof(string));
                    accountCPMStatusByPolicyDataTable.Columns.Add("Success", typeof(int));
                    accountCPMStatusByPolicyDataTable.Columns.Add("Failure", typeof(int));
                    accountCPMStatusByPolicyDataTable.Columns.Add("CPM Disabled", typeof(int));
                    queryString = "With result as (select PolicyID, count(case CPMStatus when 'success' then 1 else null end) as 'Success', count(case CPMStatus when 'failure' then 1 else null end) as 'Failure', count(case 'x' when 0 then null else null end) as 'CPMDisabled' from (select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue as 'CPMStatus' from objectproperties,files where files.Type == 2 and files.DeletedBy == '' and objectproperties.ObjectPropertyName == 'CPMStatus' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID  COLLATE NOCASE) t1 LEFT JOIN(select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue AS 'PolicyID' from objectproperties, files where files.Type == 2 and files.DeletedBy == '' and objectproperties.ObjectPropertyName == 'PolicyID' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID  COLLATE NOCASE) t2 ON t1.SafeID == t2.SafeID and t1.FileID == t2.FileID group by PolicyID union select PolicyID, count(case 'x' when 0 then null else null end) as 'Success', count(case 'y' when 0 then null else null end) as 'Failure', count(CPMDisabled) as 'CPMDisabled'from(select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyName as 'CPMDisabled' from objectproperties, files where objectproperties.ObjectPropertyName == 'CPMDisabled' and files.SafeID = objectproperties.SafeID and files.FileID == objectproperties.FileID and files.DeletedBy == '' COLLATE NOCASE) t1 LEFT JOIN(select objectproperties.FileID, objectproperties.SafeID, objectproperties.ObjectPropertyValue AS 'PolicyID' from objectproperties, files where files.DeletedBy == '' and objectproperties.ObjectPropertyName == 'PolicyID' and objectproperties.FileID == files.FileID and objectproperties.SafeID = files.SafeID COLLATE NOCASE) t2 ON t1.SafeID == t2.SafeID and t1.FileID == t2.FileID group by PolicyID) select PolicyID, Sum(Success) as 'Success', Sum(Failure) as 'Failure', Sum(CPMDisabled) as 'CPM Disabled' from result group by PolicyID order by Success desc";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(accountCPMStatusByPolicyDataTable);
                    DBFunctions.closeDBConnection();

                    accountCPMStatusByPolicyDataTable.Columns.Add("Success (%)", typeof(double));
                    accountCPMStatusByPolicyDataTable.Columns.Add("Failure (%)", typeof(double));
                    accountCPMStatusByPolicyDataTable.Columns.Add("CPM Disabled (%)", typeof(double));
                    accountCPMStatusByPolicyDataTable.Columns.Add("Assigned Accounts", typeof(int));
                    for (int i = 0; i < accountCPMStatusByPolicyDataTable.Rows.Count; i++)
                    {
                        for (int j = 0; j < accountsPerPolicyDataTable.Rows.Count; j++)
                        {
                            if (accountCPMStatusByPolicyDataTable.Rows[i][0].Equals(accountsPerPolicyDataTable.Rows[j][0]))
                            {
                                accountCPMStatusByPolicyDataTable.Rows[i][7] = accountsPerPolicyDataTable.Rows[j][1];
                                accountCPMStatusByPolicyDataTable.Rows[i][4] = Math.Round((int)accountCPMStatusByPolicyDataTable.Rows[i][1] * 100.00 / (int)accountsPerPolicyDataTable.Rows[j][1], 2);
                                accountCPMStatusByPolicyDataTable.Rows[i][5] = Math.Round((int)accountCPMStatusByPolicyDataTable.Rows[i][2] * 100.00 / (int)accountsPerPolicyDataTable.Rows[j][1], 2);
                                accountCPMStatusByPolicyDataTable.Rows[i][6] = Math.Round((int)accountCPMStatusByPolicyDataTable.Rows[i][3] * 100.00 / (int)accountsPerPolicyDataTable.Rows[j][1], 2);
                            }
                        }
                    }


                    accountCPMStatusByPolicyDataTable.Columns["Success (%)"].SetOrdinal(2);
                    accountCPMStatusByPolicyDataTable.Columns["Failure (%)"].SetOrdinal(4);
                    accountCPMStatusByPolicyDataTable.Columns["CPM Disabled (%)"].SetOrdinal(6);


                    accountsDevelopmentDataTable = (from table1 in objectsCountByPeriod.AsEnumerable()
                                                    join table2 in allObjectsGroupedByCreationDate.AsEnumerable()
                                                    on table1.Field<string>("Date") equals table2.Field<string>("Date")
                                                    join table3 in allDeletedObjectsGroupedByDeletionDate.AsEnumerable()
                                                    on table2.Field<string>("Date") equals table3.Field<string>("Date")
                                                    select new
                                                    {
                                                        Date = table1.Field<string>("Date"),
                                                        NumberOfAccounts = table1.Field<int>("Accounts"),
                                                        CreatedAccounts = table2.Field<int>("Accounts"),
                                                        DeletedAccounts = table3.Field<int>("Accounts")
                                                    }).ToDataTable();


                    disabledAccounts = accountCPMStatusByPolicyDataTable.AsEnumerable().Sum(x => Convert.ToInt32(x["CPM Disabled"]));

                    accountsDisabledBy.Columns.Add("DisabledBy", typeof(string));
                    accountsDisabledBy.Columns.Add("NumberOfAccounts", typeof(int));
                    accountsDisabledBy.Columns.Add("Share (%)", typeof(double));
                    accountsDisabledBy.Rows.Add("CPM", cpmDisabledAccounts, Math.Round(cpmDisabledAccounts * 100.00 / disabledAccounts, 2));
                    accountsDisabledBy.Rows.Add("Users", userDisabledAccounts, Math.Round(userDisabledAccounts * 100.00 / disabledAccounts, 2));


                    cpmInformationDataTable.Columns.Add("CPM", typeof(string));
                    cpmInformationDataTable.Columns.Add("Safes", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalSafesShare (%)", typeof(double));
                    cpmInformationDataTable.Columns.Add("Policies", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalUsedPoliciesShare (%)", typeof(double));
                    cpmInformationDataTable.Columns.Add("Accounts", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalAccountsShare (%)", typeof(double));
                    cpmInformationDataTable.Columns.Add("ManagedAccounts", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalManagedAccountsShare (%)", typeof(double));
                    cpmInformationDataTable.Columns.Add("CompliantAccounts", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalCompliantAccountsShare (%)", typeof(double));
                    cpmInformationDataTable.Columns.Add("Non-CompliantAccounts", typeof(int));
                    cpmInformationDataTable.Columns.Add("TotalNon-CompliantAccountsShare (%)", typeof(double));

                    for (int i = 0; i < CPMs.Count; i++)
                    {

                        int safes = 0;
                        if (CPMs[i] != "No CPM" && CPMs[i] != "Several CPMs")
                        {
                            safes = cpmOwnerships[CPMs[i]].Count();
                        }
                        else
                        {
                            safes = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" select row.Field<string>("SafeName")).Distinct().Count();
                        }

                        int policies = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" && !row.Field<string>("SafeName").ToLower().Contains("_workspace") && row.Field<string>("Deleted") == "No" && row.Field<string>("PolicyID") != null select row.Field<string>("PolicyID")).Distinct().Count();
                        int accounts = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" && !row.Field<string>("SafeName").ToLower().Contains("_workspace") && row.Field<string>("Deleted") == "No" select row.Field<string>("AccountName")).Count();
                        int managed = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" && row.Field<string>("Managed") == "Yes" select row.Field<string>("Managed")).Count();
                        int compliant = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" && row.Field<string>("Compliant") == "Yes" select row.Field<string>("Compliant")).Count();
                        int nonCompliant = (from row in accountsDataTable.AsEnumerable() where row.Field<string>(CPMs[i]) == "Yes" && row.Field<string>("Compliant") == "No" select row.Field<string>("Compliant")).Count();
                        cpmInformationDataTable.Rows.Add(CPMs[i], safes, Math.Round(safes * 100.00 / numberOfSafes, 2), policies, Math.Round(policies * 100.00 / numberOfPolicies, 2), accounts, Math.Round(accounts * 100.00 / effectiveNumberOfAccounts, 2), managed, Math.Round(managed * 100.00 / managedAccounts, 2), compliant, Math.Round(compliant * 100.00 / compliantAccounts, 2), nonCompliant, Math.Round(nonCompliant * 100.00 / nonCompliantAccounts, 2));
                    }


                    cpmInformationDataTable.DefaultView.Sort = "Accounts DESC";
                    cpmInformationDataTable = cpmInformationDataTable.DefaultView.ToTable();


                    accountStatisticsDataTable.Rows.Add("Total number of accounts", string.Format("{0:#,##0}", effectiveNumberOfAccounts), "excluding " + numberOfDeletedAccounts + " deleted accounts, " + temporaryAccountObjects + " temporary CPM account objects, " + tempAdHocAccounts + " Ad-Hoc connection account objects, and " + totalPendingAccounts + " pending accounts");
                    accountStatisticsDataTable.Rows.Add("Managed accounts", string.Format("{0:#,##0}", managedAccounts), "" + Math.Round(managedAccounts * 100.00 / effectiveNumberOfAccounts, 2) + " % | Managed account = account that is not disabled manually (by human users) AND that is assigned to a policy which has periodic change enabled AND has at least one none-disabled CPM assigned as owner on its safe AND can be accessed by CPMs according to the AllowedSafes platform policy setting");
                    accountStatisticsDataTable.Rows.Add("Compliant accounts", string.Format("{0:#,##0}", compliantAccounts), "" + Math.Round(compliantAccounts * 100.00 / managedAccounts, 2) + " % of managed accounts");
                    accountStatisticsDataTable.Rows.Add("Non-compliant accounts", string.Format("{0:#,##0}", nonCompliantAccounts), "" + Math.Round(nonCompliantAccounts * 100.00 / managedAccounts, 2) + " % of managed accounts");
                    accountStatisticsDataTable.Rows.Add("Deleted accounts", string.Format("{0:#,##0}", numberOfDeletedAccounts), "Accounts that are marked for deletion excluding " + temporaryDeletedAccountObjects + " deleted temporary account objects");
                    if (totalPendingAccounts > 0)
                    {
                        accountStatisticsDataTable.Rows.Add("Pending accounts", string.Format("{0:#,##0}", totalPendingAccounts));
                    }
                    accountStatisticsDataTable.Rows.Add("Number of policies", "" + numberOfPolicies, "Policies that have accounts assigned");
                    if (tempAdHocAccounts == 0)
                    {
                        accountStatisticsDataTable.Rows.Add("Accounts assigned to policies", string.Format("{0:#,##0}", accountsAssignedToPolicies), "" + Math.Round(accountsAssignedToPolicies * 100.00 / effectiveNumberOfAccounts, 2) + " %");
                    }
                    else
                    {
                        accountStatisticsDataTable.Rows.Add("Accounts assigned to policies", string.Format("{0:#,##0}", accountsAssignedToPolicies), "" + Math.Round(accountsAssignedToPolicies * 100.00 / effectiveNumberOfAccounts, 2) + " % - excluding " + tempAdHocAccounts + " Ad-Hoc connection accounts");
                    }

                    accountStatisticsDataTable.Rows.Add("Accounts with no policy assignment", string.Format("{0:#,##0}", accountsWithNoPolicyAssignment), "" + Math.Round(accountsWithNoPolicyAssignment * 100.00 / effectiveNumberOfAccounts, 2) + " %");
                    accountStatisticsDataTable.Rows.Add("Disabled accounts", string.Format("{0:#,##0}", disabledAccounts), "" + Math.Round(disabledAccounts * 100.00 / effectiveNumberOfAccounts, 2) + " %");
                    accountStatisticsDataTable.Rows.Add("Accounts disabled by CPM", string.Format("{0:#,##0}", cpmDisabledAccounts), "" + Math.Round(cpmDisabledAccounts * 100.00 / disabledAccounts, 2) + " % of all disabled accounts");
                    accountStatisticsDataTable.Rows.Add("Accounts disabled by users", string.Format("{0:#,##0}", userDisabledAccounts), "" + Math.Round(userDisabledAccounts * 100.00 / disabledAccounts, 2) + " % of all disabled accounts");
                    accountStatisticsDataTable.Rows.Add("Average accounts per safe", effectiveNumberOfAccounts / numberOfSafes, "rounded to whole number");
                    accountStatisticsDataTable.Rows.Add("Average created accounts per month", numberOfAccounts / monthsScale, "The number includes accounts that were deleted in the meantime. Rounded to whole number");
                    accountStatisticsDataTable.Rows.Add("Average deleted accounts per month", string.Format("{0:#,##0}", Math.Round((double)numberOfDeletedAccounts / monthsScale, 2)), "");
                    accountStatisticsDataTable.Rows.Add("Average account growth per month", string.Format("{0:#,##0}", effectiveNumberOfAccounts / monthsScale), "Effective accounts growth per month");

                    if (monthsScale > 11)
                    {
                        accountStatisticsDataTable.Rows.Add("Average created accounts per year", string.Format("{0:#,##0}", Math.Round(numberOfAccounts / monthsScale * 12.00, 2)), "The number includes accounts that were deleted in the meantime");
                        accountStatisticsDataTable.Rows.Add("Average deleted accounts per year", string.Format("{0:#,##0}", Math.Round(numberOfDeletedAccounts / monthsScale * 12.00, 2)), "");
                        accountStatisticsDataTable.Rows.Add("Average account growth per year", string.Format("{0:#,##0}", effectiveNumberOfAccounts / monthsScale * 12.00), "Effective accounts growth per year");
                    }


                    if (hasPlatformPolicies && hasMasterPolicy && hasFiles && isNotNullOrEmpty(platformPoliciesTable))
                    {
                        try
                        {
                            DBFunctions.storeDataTableInSqliteDatabase(platformPoliciesTable, "PlatformpoliciesTable");
                            queryString = "Select 'Session monitoring and isolation' as Setting, Case When [Require privileged session monitoring and isolation] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Require privileged session monitoring and isolation] in ('Active','Inactive') and Accounts > 0 Group by [Require privileged session monitoring and isolation] Union all Select 'Record and save session activity' as Setting, Case When [Record and save session activity] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Record and save session activity] in ('Active','Inactive') and Accounts > 0 Group by [Record and save session activity] Union all Select 'One-time password' as Setting, Case When [Enforce one-time password access] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Enforce one-time password access] in ('Active','Inactive') and Accounts > 0 Group by [Enforce one-time password access] Union all Select 'Exclusive access' as Setting, Case When [Enforce check-in/check-out exclusive access] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Enforce check-in/check-out exclusive access] in ('Active','Inactive') and Accounts > 0 Group by [Enforce check-in/check-out exclusive access] Union all Select 'Dual control approval' as Setting, Case When [Require dual control password access approval] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Require dual control password access approval] in ('Active','Inactive') and Accounts > 0 Group by [Require dual control password access approval] Union all Select 'Multi-level approval' as Setting, Case When [Require multi-level password access approval] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Require multi-level password access approval] in ('Active','Inactive') and Accounts > 0 Group by [Require multi-level password access approval] Union all Select 'Only direct managers can approve' as Setting, Case When [Only direct managers can approve password access requests] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Only direct managers can approve password access requests] in ('Active','Inactive') and Accounts > 0 Group by [Only direct managers can approve password access requests] Union all Select 'Require reason for access' as Setting, Case When [Require users to specify reason for access] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Require users to specify reason for access] in ('Active','Inactive') and Accounts > 0 Group by [Require users to specify reason for access] Union all Select 'Allow transparent connections' as Setting, Case When [Allow EPV transparent connections ('Click to connect')] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Allow EPV transparent connections ('Click to connect')] in ('Active','Inactive') and Accounts > 0 Group by [Allow EPV transparent connections ('Click to connect')] Union all Select 'Allow users to view passwords' as Setting, Case When [Allow users to view passwords] = 'Active' Then 'Yes' Else 'No' End as Active, Count(*) as Policies, Sum(Accounts) as Accounts from PlatformpoliciesTable where [Allow users to view passwords] in ('Active','Inactive') and Accounts > 0 Group by [Allow users to view passwords]";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(masterPolicySummaryTable);
                            DBFunctions.closeDBConnection();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(DateTime.Now + " An error occurred while trying to determine master policy summary information");
                        }
                    }



                    filesSizeGroupedbyCreationDate = new DataTable();
                    safesStatisticsDataTable = new DataTable();
                    safesStatisticsDataTable.Columns.Add("Description", typeof(string));
                    safesStatisticsDataTable.Columns.Add("Statistic", typeof(string));
                    safesStatisticsDataTable.Columns.Add("Comments", typeof(string));
                    safesStatisticsDataTable.Rows.Add("Total number of safes", string.Format("{0:#,##0}", numberOfSafes));
                    safesStatisticsDataTable.Rows.Add("Total number of objects", string.Format("{0:#,##0}", effectiveNumberOfObjects), "Excluding " + string.Format("{0:#,##0}", numberOfDeletedObjects) + " deleted objects");
                    if (recordingMetaDataFiles > 0)
                    {
                        safesStatisticsDataTable.Rows.Add("Recording metadata objects", string.Format("{0:#,##0}", recordingMetaDataFiles), "Excluding " + string.Format("{0:#,##0}", deletedRecordingMetaDataFiles) + " deleted recording metadata objects");
                    }
                    safesStatisticsDataTable.Rows.Add("Total number of files", string.Format("{0:#,##0}", effectiveNumberOfFiles), "Excluding " + string.Format("{0:#,##0}", numberOfDeletedFiles) + " deleted files");
                    safesStatisticsDataTable.Rows.Add("Total number of accounts", string.Format("{0:#,##0}", effectiveNumberOfAccounts), "Excluding " + temporaryAccountObjects + " temporary CPM account objects and " + tempAdHocAccounts + " Ad-Hoc connection account objects");
                    safesStatisticsDataTable.Rows.Add("Average objects per safe",
                        effectiveNumberOfObjects / numberOfSafes, "rounded to whole number");
                    safesStatisticsDataTable.Rows.Add("Average accounts per safe",
                        effectiveNumberOfAccounts / numberOfSafes, "rounded to whole number");
                    safesStatisticsDataTable.Rows.Add("Average files per safe",
                        effectiveNumberOfFiles / numberOfSafes, "rounded to whole number");
                    safesStatisticsDataTable.Rows.Add("Total size of all objects",
                        totalSizeOfAllSafes + " GB (" + Math.Round(totalSizeOfAllSafes * 100 / totalMaxSize, 2) +
                        "%)");
                    safesStatisticsDataTable.Rows.Add("Average safe used size:", averageSafeSize + " GB");
                    safesStatisticsDataTable.Rows.Add("Total max size of all safes:", totalMaxSize + " GB");
                    safesStatisticsDataTable.Rows.Add("Average safe max size:", averageSafeMaxSize + " GB");
                    safesStatisticsDataTable.Rows.Add("Total free safe space:",
                        totalMaxSize - totalSizeOfAllSafes + " GB (" +
                        Math.Round((100 - (totalSizeOfAllSafes * 100 / totalMaxSize)), 2) + "%)");
                    safesStatisticsDataTable.Rows.Add("Average free safe space:",
                        Math.Round((double)(totalMaxSize - totalSizeOfAllSafes) / numberOfSafes, 2) + " GB");
                    safesStatisticsDataTable.Rows.Add("Average safes creation:",
                        (numberOfSafes / monthsScale) + " safes (per month)");
                    safesStatisticsDataTable.Rows.Add("Average objects creation:",
                        (numberOfObjects / monthsScale) + " objects (per month)");
                    safesStatisticsDataTable.Rows.Add("Average files creation:",
                        (numberOfFiles / monthsScale) + " files (per month)");
                    safesStatisticsDataTable.Rows.Add("Average data creation:",
                        Math.Round(totalSizeOfAllSafes / monthsScale, 2) + " GB (per month)");
                    da.Dispose();


                    if (hasFiles)
                    {
                        queryString = "Create table if not exists Recordings as select a.SessionID, h.UserName, h.UserTypeID, a.SafeName, Case when b.VideoRecording is not null or c.Keystrokes is not null or d.[SQL Recording] is not null or e.[Windows Actions] is not null or h.UserTypeID = 36 then 'PSM' when a.Filename like '________-____-____-____-____________.txt' then 'OPM' when h.UserTypeID  = 70 Then 'PSM for SSH' else (CASE WHEN CreatedBy Like 'PSMA%' Then 'PSM' WHEN CreatedBy Like 'PSM-APP%' Then 'PSM' WHEN CreatedBy Like 'PSM_APP%' Then 'PSM' WHEN CreatedBy Like 'PSMPA%' Then 'PSM for SSH' When CreatedBy Like 'PSMP_APP%' Then 'PSM for SSH' When CreatedBy Like 'PSMP-APP%' Then 'PSM for SSH' When CreatedBy Like 'OPM%' Then 'OPM' Else 'Unknown' END) END AS [PSM Solution], a.CreationDate, a.CreatedBy, Case when b.VideoRecording is null then 'No' else 'Yes' end as VideoRecording, Replace(b.VideoRecordingSize,'\"','') as VideoRecordingSize, case when c.KeyStrokes is null then 'No' else 'Yes' end as KeyStrokesRecording, Replace(c.KeystrokesSize,'\"','') as KeystrokesSize, case when d.[SQL Recording] is null then 'No' else 'Yes' end as [SQL Recording], Replace(d.SQLRecordingSize,'\"','') as SQLRecordingSize, Case when e.[Windows Actions] is null then 'No' else 'Yes' End as [WindowsActionsRecording], Replace(e.WindowsActionsSize,'\"','') as WindowsActionsSize, case when f.[SSH Recording] is Null then 'No' else 'Yes' end as [SSH Recording], Replace(f.SSHRecordingSize,'\"','') as SSHRecordingSize, Case when g.[OPM Recording] is null then 'No' else 'Yes' end as [OPM Recording], Replace(g.OPMRecordingSize,'\"','') as OPMRecordingSize from (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName, SafeName, CreationDate, CreatedBy from files where (FileName Like '%-%-%-%-%.%.txt' OR FileName Like '%-%-%-%-%.%.avi' OR FileName Like '________-____-____-____-____________.txt') and type = 1 and deletedBy = '' group by SessionID) a left join (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as VideoRecording, CompressedSize as VideoRecordingSize from files where FileName Like '%-%-%-%-%.%.avi' and type = 1 and deletedBy = '') b on a.SessionID = b.SessionID left join (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as KeyStrokes, CompressedSize as KeystrokesSize from files where FileName Like '%-%-%-%-%.Keystrokes.txt' and type = 1 and deletedBy = '') c on a.SessionID = c.SessionID left join (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as [SQL Recording], CompressedSize as SQLRecordingSize from files where FileName Like '%-%-%-%-%.SQL.txt' and type = 1 and deletedBy = '') d on a.SessionID = d.SessionID left join (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as [Windows Actions], CompressedSize as WindowsActionsSize from files where FileName Like '%-%-%-%-%.WIN.txt' and type = 1 and deletedBy = '') e on a.SessionID = e.SessionID left join (select distinct substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as [SSH Recording], CompressedSize as SSHRecordingSize from files where FileName Like '%-%-%-%-%.SSH.txt' and type = 1 and deletedBy = '') f on a.SessionID = f.SessionID left join (select substr(FileName, 1, instr(FileName, '.') - 1) as SessionID, FileName as [OPM Recording], CompressedSize as OPMRecordingSize from files where FileName Like '________-____-____-____-____________.txt' and type = 1 and deletedBy = '') g on a.SessionID = g.SessionID left join (select UserName, UserTypeID from Users) h on a.CreatedBy = h.UserName group by a.SessionID";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        tmpTable = new DataTable();
                        da.Fill(tmpTable);
                        DBFunctions.closeDBConnection();


                        // Determining session recordings information...

                        queryString = "Create table RecordingFiles as select FileName, SafeName, Folder, Case when a.UserTypeID = 70 then 'PSM for SSH' when a.UserTypeID = 36 then 'PSM' when a.UserTypeID =  37 then 'OPM' else (CASE WHEN CreatedBy Like 'PSMA%' Then 'PSM' WHEN CreatedBy Like 'PSM-APP%' Then 'PSM' WHEN CreatedBy Like 'PSM_APP%' Then 'PSM' WHEN CreatedBy Like 'PSMPA%' Then 'PSM for SSH' When CreatedBy Like 'PSMP_APP%' Then 'PSM for SSH' When CreatedBy Like 'PSMP-APP%' Then 'PSM for SSH' When CreatedBy Like 'OPM%' Then 'OPM' Else (CASE WHEN FileName not like '%-%-%-%-%.SSH.txt' Then 'PSM' Else (CASE WHEN (select count(fileName) from files where fileName Like (substr(FileName, 1, instr(FileName, '.') - 1) || '%')) > 1 Then 'PSM' Else 'PSM for SSH' END) End) END) END AS 'Solution', Case when FileName Like '%-%-%-%-%.%.avi' then 'Video recording' when FileName like '%-%-%-%-%.SSH.txt' then 'SSH recording' when FileName Like '%-%-%-%-%.Keystrokes.txt' then 'Keystrokes recording' when FileName Like '%-%-%-%-%.WIN.txt' then 'Windows actions recording' when FileName Like '%-%-%-%-%.SQL.txt' then 'SQL recording' when Filename like '________-____-____-____-____________.txt' then 'OPM recording' else 'Unknown' end as RecordingType, DateTime(CreationDate) as CreationDate, Round(Julianday($date) - Julianday(DateTime(CreationDate)),-1) as [Age (days)], CreatedBy, Cast(Trim(Replace(CompressedSize, '\"', '')) as decimal) as CompressedSize, Case When DeletedBy = '' Then 'No' else 'Yes' end as Deleted, DeletedBy, Case When DeletionDate != '0001-01-01 00:00:00' then Date(DeletionDate) else Null end as DeletionDate, substr(FileName, 1, instr(FileName, '.') - 1) as SessionID from files left join (select Users.UserName, Users.UserTypeID from Users) a on Files.CreatedBy = a.UserName where type = 1 and FileName Like '%-%-%-%-%.SSH.txt' or FileName Like '________-____-____-____-____________.txt' or FileName Like '%-%-%-%-%.WIN.txt' or  FileName Like '%-%-%-%-%.%.txt' OR FileName Like '%-%-%-%-%.%.avi' OR FileName Like '%-%-%-%-%.SQL.txt' order by [Age (days)] desc";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        command.Parameters.AddWithValue("$date", fileDate);
                        da = new SQLiteDataAdapter(command);
                        da.Fill(recordingFiles);
                        DBFunctions.closeDBConnection();


                        tempDataTable = new DataTable();
                        queryString = "Create INDEX if not exists i_RecordingFiles_1 ON RecordingFiles(SessionID);";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();


                        queryString = "Create table sessionRecordings as Select * From (select a.[CyberArk User], SafeName, Folder, CreatedBy, case when a.UserTypeID = 70 then 'PSM for SSH' when a.UserTypeID = 36 then 'PSM' when a.UserTypeID =  37 then 'OPM' when a.ConnectionComponent like 'PSM-%' then 'PSM' when a.ConnectionComponent like 'PSMP%' then 'PSM for SSH' else (CASE WHEN CreatedBy Like 'PSMA%' Then 'PSM' WHEN CreatedBy Like 'PSM-APP%' Then 'PSM' WHEN CreatedBy Like 'PSM_APP%' Then 'PSM' WHEN CreatedBy Like 'PSMPA%' Then 'PSM for SSH' When CreatedBy Like 'PSMP_APP%' Then 'PSM for SSH' When CreatedBy Like 'PSMP-APP%' Then 'PSM for SSH' When CreatedBy Like 'OPM%' Then 'OPM' Else 'Unknown' END) END AS 'Solution', DateTime(CreationDate) as CreationDate, Round(Julianday($date) - Julianday(DateTime(CreationDate)),-1) as [Age (days)], a.PolicyID, a.DeviceType, Trim(a.TargetHost) as TargetHost, Case when a.Address = a.TargetHost then '' else a.Address end as Address, a.TargetUser, a.ConnectionComponent, a.PSMSourceAddress, a.Protocol, Case when a.Database != '' then a.Database else '-' end as Database, DateTime(a.PSMStartTime, 'unixepoch') as PSMStartTime, DateTime(a.PSMEndTime, 'unixepoch') as PSMEndTime, (select count(FileName) from RecordingFiles where RecordingFiles.SessionID = substr(Files.FileName, 1, instr(Files.FileName, '.') - 1)) RecordingFiles, a.Status, a.ClientApp, Case When DeletedBy = '' Then 'No' else 'Yes' end as Deleted, DeletedBy, Case when DeletedBy = '' then null else DeletionDate end as DeletionDate, substr(FileName, 1, instr(FileName, '.') - 1) as SessionID from files left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as PolicyID from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PolicyID') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as DeviceType from ObjectProperties where ObjectProperties.ObjectPropertyName = 'DeviceType') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as TargetHost from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMRemoteMachine') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as Address from ObjectProperties where ObjectProperties.ObjectPropertyName = 'Address') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as ConnectionComponent from ObjectProperties where ObjectProperties.ObjectPropertyName = 'ConnectionComponentID') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as ClientApp from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMClientApp') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as PSMStartTime from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMStartTime') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as PSMEndTime from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMEndTime') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as [CyberArk User] from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMVaultUserName') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as TargetUser from ObjectProperties where ObjectProperties.ObjectPropertyName = 'UserName') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as Status from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMStatus') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as Protocol from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMProtocol') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as PSMSourceAddress from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMSourceAddress') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as RecordingFiles from ObjectProperties where ObjectProperties.ObjectPropertyName = 'ActualRecordings') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as Database from ObjectProperties where ObjectProperties.ObjectPropertyName = 'Database') a using (FileID,SafeID) left join (select ObjectProperties.SafeID, ObjectProperties.FileID, ObjectProperties.ObjectPropertyName, ObjectProperties.ObjectPropertyValue as PSMRecordingEntity from ObjectProperties where ObjectProperties.ObjectPropertyName = 'PSMRecordingEntity') a using (FileID,SafeID) left join (select Users.UserName, Users.UserTypeID from Users) a on Files.CreatedBy = a.UserName where files.FileName like '%.session') Where RecordingFiles != 0 order By CreationDate";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        command.Parameters.AddWithValue("$date", fileDate);
                        da = new SQLiteDataAdapter(command);
                        tempDataTable = new DataTable();
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();


                        queryString = "select * from sessionRecordings";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionRecordings);
                        DBFunctions.closeDBConnection();


                        allRecordings = sessionRecordings.Rows.Count;

                        sessionRecordings.Columns.Add("RecordingDuration", typeof(TimeSpan)).SetOrdinal(sessionRecordings.Columns["PSMEndTime"].Ordinal + 1);

                        for (int i = 0; i < sessionRecordings.Rows.Count; i++)
                        {
                            if (sessionRecordings.Rows[i]["PSMEndTime"] != DBNull.Value && sessionRecordings.Rows[i]["PSMStartTime"] != DBNull.Value)
                            {

                                sessionRecordings.Rows[i]["RecordingDuration"] = (Convert.ToDateTime(sessionRecordings.Rows[i]["PSMEndTime"]) - Convert.ToDateTime(sessionRecordings.Rows[i]["PSMStartTime"]));

                            }
                        }

                        if (allRecordings > 0)
                        {


                            hasRecordings = true;
                            queryString = "select [PSM Solution], Count(*) as Recordings, (Total(VideoRecordingSize)  + Total(KeystrokesSize) + Total(SQLRecordingSize) + Total(WindowsActionsSize) + Total(SSHRecordingSize) + Total(OPMRecordingSize)) /1024.0/1024.0/1024.0 as Size from Recordings group by [PSM Solution] order by Recordings desc";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(recordingsByPsmSolution);
                            DBFunctions.closeDBConnection();


                            allRecordingsSize = recordingsByPsmSolution.AsEnumerable().Sum(x => x.Field<double>("Size"));

                            queryString = "Select SafeName, Count(*) as Recordings, Round((Total(VideoRecordingSize)  + Total(KeystrokesSize) + Total(SQLRecordingSize) + Total(WindowsActionsSize) + Total(SSHRecordingSize) + Total(OPMRecordingSize)) /1024.0/1024.0 ,2) as [RecordingsSize (MB)] from Recordings group by SafeName order by Recordings desc";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(recordingsPerSafe);
                            DBFunctions.closeDBConnection();

                            queryString = "Select Solution, count(*) as RecordingFiles from RecordingFiles where Deleted = 'No' group by Solution";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(recordingFilesBySolution);
                            DBFunctions.closeDBConnection();


                            queryString = "Select count(*) as RecordingFiles from RecordingFiles where Deleted = 'No'";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            allRecordingFiles = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();


                            queryString = "select min(CreationDate) from Recordings";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            tmpTable = new DataTable();
                            da = new SQLiteDataAdapter(command);
                            da.Fill(tmpTable);
                            DBFunctions.closeDBConnection();
                            DateTime minRecordingDate = new DateTime();
                            minRecordingDate = DateTime.Parse(tmpTable.Rows[0][0].ToString());


                            queryString = "select max(CreationDate) from Recordings";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            tmpTable = new DataTable();
                            da = new SQLiteDataAdapter(command);
                            da.Fill(tmpTable);
                            DBFunctions.closeDBConnection();
                            DateTime maxRecordingDate = new DateTime();
                            maxRecordingDate = DateTime.Parse(tmpTable.Rows[0][0].ToString());


                            minRecordingDate = minRecordingDate.Date;
                            maxRecordingDate = maxRecordingDate.Date.AddMinutes(1439).AddSeconds(59);


                            recordingsDevelopment.Columns.Add("Date", typeof(DateTime));
                            recordingsDevelopment.Columns.Add("Recordings", typeof(int));
                            recordingsDevelopment.Columns.Add("Size", typeof(double));
                            queryString = "select Count(*) as Recordings, Total(VideoRecordingSize)  + Total(KeystrokesSize) + Total(SQLRecordingSize) + Total(WindowsActionsSize) + Total(SSHRecordingSize) + Total(OPMRecordingSize) as Size from Recordings where CreationDate < $date";
                            using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                            {
                                con.Open();
                                using (SQLiteCommand cmd = con.CreateCommand())
                                {
                                    cmd.CommandText = queryString;
                                    da = new SQLiteDataAdapter(cmd);

                                    while (minRecordingDate <= maxRecordingDate)
                                    {
                                        tmpTable = new DataTable();
                                        cmd.Parameters.AddWithValue("$date", minRecordingDate.AddDays(1));
                                        da.Fill(tmpTable);
                                        recordingsDevelopment.Rows.Add(minRecordingDate, Convert.ToInt32(tmpTable.Rows[0][0]), Convert.ToDouble(tmpTable.Rows[0][1]));

                                        minRecordingDate = minRecordingDate.AddDays(1);
                                    }

                                }
                                con.Close();
                            }


                            recordingsDevelopment = (from r in recordingsDevelopment.AsEnumerable()
                                                     group r by r.Field<DateTime>("Date").Date.Month.ToString("D2") + "/" + r.Field<DateTime>("Date").Date.Year into g
                                                     select new
                                                     {
                                                         Date = g.Key,
                                                         Recordings = g.Select(x => x.Field<int>("Recordings")).Last(),
                                                         Size = Math.Round(g.Select(x => x.Field<double>("Size")).Last() / 1024.00 / 1024.00 / 1024.00, 2)
                                                     }).ToDataTable();


                            recordingsSizeDevelopment = recordingsDevelopment.Copy();
                            recordingsDevelopment.Columns.Remove("Size");
                            recordingsSizeDevelopment.Columns.Remove("Recordings");
                            recordingsSizeDevelopment.Columns["Size"].ColumnName = "Size (GB)";

                        }

                    }


                    // Determining permissions information...


                    usersAndGroupsPermissions = usersDataTable.Copy();
                    usersAndGroupsPermissions.TableName = "UsersAndGroupsPermissions";

                    usersAndGroupsPermissions.Columns.Add("User/Group", typeof(string));
                    usersAndGroupsPermissions.Columns.Add("Information", typeof(string));
                    usersAndGroupsPermissions.Columns.Add("Permissions", typeof(bool));
                    usersAndGroupsPermissions.Columns.Add("HasPermissions", typeof(string));
                    usersAndGroupsPermissions.Columns.Add("ButtonText", typeof(string));

                    usersAndGroupsPermissions.Columns.Remove("AllAuthorizations");
                    usersAndGroupsPermissions.Columns.Remove("Add/UpdateUsers");
                    usersAndGroupsPermissions.Columns.Remove("AddSafes");
                    usersAndGroupsPermissions.Columns.Remove("ManageDirectoryMappings");
                    usersAndGroupsPermissions.Columns.Remove("AddNetworkAreas");
                    usersAndGroupsPermissions.Columns.Remove("ManageServerFileCategories");
                    usersAndGroupsPermissions.Columns.Remove("AuditUsers");
                    usersAndGroupsPermissions.Columns.Remove("BackupAllSafes");
                    usersAndGroupsPermissions.Columns.Remove("RestoreAllSafes");
                    usersAndGroupsPermissions.Columns.Remove("ResetUserPasswords");
                    usersAndGroupsPermissions.Columns.Remove("ActivateUsers");
                    usersAndGroupsPermissions.Columns.Remove("CreationDate");
                    usersAndGroupsPermissions.Columns.Remove("PrevLogonDaysAgo");
                    usersAndGroupsPermissions.Columns.Remove("RestrictedInterfaces");
                    usersAndGroupsPermissions.Columns.Remove("FromHour");
                    usersAndGroupsPermissions.Columns.Remove("ToHour");
                    usersAndGroupsPermissions.Columns.Remove("PrevLogonDate");
                    usersAndGroupsPermissions.Columns.Remove("PartialImpersonation");
                    usersAndGroupsPermissions.Columns.Remove("FullImpersonation");
                    usersAndGroupsPermissions.Columns.Remove("ImpersonationWithServerAuthentication");
                    usersAndGroupsPermissions.Columns.Remove("LogRetentionPeriod");
                    usersAndGroupsPermissions.Columns.Remove("ApplicationMetadata");


                    usersAndGroupsPermissions.Columns["UserName"].ColumnName = "Name";
                    usersAndGroupsPermissions.Columns["UserType"].ColumnName = "Type";

                    usersAndGroupsPermissions.Columns.Add("ListRetrieveAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ListRetrieveNoConfirmAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveNoConfirmAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveNoConfirmSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveNoConfirmSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ListRetrieveViewOwnersAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveViewOwnersAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveViewOwnersSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListRetrieveViewOwnersSafes(%)", typeof(double));


                    usersAndGroupsPermissions.Columns.Add("ListUseAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListUseAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListUseSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListUseSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ListUseNoConfirmAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListUseNoConfirmAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListUseNoConfirmSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListUseNoConfirmSafes(%)", typeof(double));


                    usersAndGroupsPermissions.Columns.Add("ListViewOwnersAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListViewOwnersAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListViewOwnersSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListViewOwnersSafes(%)", typeof(double));



                    usersAndGroupsPermissions.Columns.Add("ListAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ListSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ListSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("UseAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UseAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("UseSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UseSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("RetrievePasswords", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("RetrievePasswords(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("RetrieveSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("RetrieveSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("CreateAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("CreateAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("CreateOnSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("CreateOnSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("DeleteAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("DeleteAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("DeleteSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("DeleteSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("UpdateAccountProperties", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UpdateAccountProperties(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("UpdateObjectPropertiesSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UpdateObjectPropertiesSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("RenameAccountObject", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("RenameAccountObject(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("RenameObjectSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("RenameObjectSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("UnlockAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UnlockAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("UnlockObjectSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UnlockObjectSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("SetManualPasswordInVault", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("SetManualPasswordInVault(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("UpdateObjectSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("UpdateObjectSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChange", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChange(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeWithManualPassword", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeWithManualPassword(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeWithManualPasswordSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("InitiateCPMChangeWithManualPasswordSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("MoveAccountsAndFoldersInSafe", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("MoveAccountsAndFoldersInSafe(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("MoveOnSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("MoveOnSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ConfirmPermissionRequestsForAccordingSafe", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ConfirmPermissionRequestsForAccordingSafe(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ConfirmPermissionRequestsSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ConfirmPermissionRequestsSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("AccessAccountWithoutRequiredConfirmation", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("AccessAccountWithoutRequiredConfirmation(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("NoConfirmRequiredSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("NoConfirmRequiredSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("BackupAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("BackupAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("BackupSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("BackupSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ManageSafeAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ManageSafeAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ManageSafe", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ManageSafe(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ManageSafeMembersAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ManageSafeMembersAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ManageSafeMembersSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ManageSafeMembersSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ViewSafeMembersAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ViewSafeMembersAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ViewSafeMembersSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ViewSafeMembersSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ViewAuditAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ViewAuditAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ViewAuditSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ViewAuditSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("CreateFolderAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("CreateFolderAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("CreateFolderSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("CreateFolderSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("DeleteFolderAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("DeleteFolderAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("DeleteFolderSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("DeleteFolderSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("ValidateSafeContentAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ValidateSafeContentAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("ValidateSafeContentSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("ValidateSafeContentSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("EventsListAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("EventsListAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("EventsListSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("EventsListSafes(%)", typeof(double));

                    usersAndGroupsPermissions.Columns.Add("EventsAddAccounts", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("EventsAddAccounts(%)", typeof(double));
                    usersAndGroupsPermissions.Columns.Add("EventsAddSafes", typeof(int));
                    usersAndGroupsPermissions.Columns.Add("EventsAddSafes(%)", typeof(double));

                    for (int i = 0; i < usersAndGroupsPermissions.Rows.Count; i++)
                    {
                        usersAndGroupsPermissions.Rows[i]["User/Group"] = "User";
                    }

                    for (int i = 0; i < groupsDataTable.Rows.Count; i++)
                    {
                        DataRow row = usersAndGroupsPermissions.NewRow();
                        row["Name"] = groupsDataTable.Rows[i]["GroupName"];
                        row["User/Group"] = "Group";
                        row["Information"] = groupsDataTable.Rows[i]["Information"];
                        row["Type"] = "Group";
                        row["Disabled"] = "-";
                        row["Origin"] = groupsDataTable.Rows[i]["Origin"];
                        row["LDAPDirectory"] = groupsDataTable.Rows[i]["LDAPDirectory"];
                        row["LDAPFullDN"] = groupsDataTable.Rows[i]["LDAPFullDN"];
                        row["MapName"] = groupsDataTable.Rows[i]["MapName"];
                        row["LocationName"] = groupsDataTable.Rows[i]["LocationName"];
                        row["GroupMemberships"] = groupsDataTable.Rows[i]["GroupMemberships"];
                        usersAndGroupsPermissions.Rows.Add(row);
                    }

                    string queryString1 = "Select Count(SafeName) from OwnerShips where ownername in (select groupname from memberships where Member = $name union select $name) and [Empty / OLAC permission] = 'No'";
                    string queryString3 = "Select IFNULL(Sum(Case when List = 'Yes' and Retrieve= 'Yes' then Accounts end) ,0) as ListRetrieveAccounts, Count(Distinct Case when List = 'Yes' and Retrieve= 'Yes' then SafeName end) as ListRetrieveSafes,  IFNull(Sum(Case when List = 'Yes' and Retrieve= 'Yes' and ViewOwners = 'Yes' then Accounts end),0) as ListRetrieveViewOwnersAccounts, Count(Distinct Case when List = 'Yes' and Retrieve= 'Yes' and ViewOwners = 'Yes' then SafeName end) as ListRetrieveViewOwnersSafes, IFNULL(Sum(Case when List = 'Yes' and UsePassword= 'Yes' then Accounts end),0) as ListUseAccounts, Count(Distinct Case when List = 'Yes' and UsePassword= 'Yes' then SafeName end) as ListUseSafes, IFNULL(Sum(Case when List = 'Yes' and ViewOwners = 'Yes' then Accounts end),0) as ListViewOwnersAccounts, Count(Distinct Case when List = 'Yes' and ViewOwners = 'Yes' then SafeName end) as ListViewOwnersSafes, IFNULL(Sum(Case when List = 'Yes' and Retrieve = 'Yes' and NoConfirmRequired = 'Yes' then Accounts end),0) as ListRetrieveNoConfirmAccounts, Count(Distinct Case when List = 'Yes' and Retrieve = 'Yes' and NoConfirmRequired = 'Yes' then SafeName end) as ListRetrieveNoConfirmSafes, IFNULL(Sum(Case when List = 'Yes' and UsePassword = 'Yes' and NoConfirmRequired = 'Yes' then Accounts end),0) as ListUseNoConfirmAccounts, Count(Distinct Case when List = 'Yes' and UsePassword = 'Yes' and NoConfirmRequired = 'Yes' then SafeName end) as ListUseNoConfirmSafes, IFNULL(Sum(Case when List = 'Yes' then Accounts end),0) as ListAccounts, Count(Distinct Case when List = 'Yes' then SafeName end) as ListSafes, IFNULL(Sum(Case when CreateObject = 'Yes' then Accounts end),0) as CreateAccounts, Count(Distinct Case when CreateObject = 'Yes' then SafeName end) as CreateOnSafes, IFNULL(Sum(Case when UsePassword = 'Yes' then Accounts end),0) as UseAccounts, Count(Distinct Case when UsePassword = 'Yes' then SafeName end) as UseSafes, IFNULL(Sum(Case when Retrieve = 'Yes' then Accounts end),0) as RetrievePasswords, Count(Distinct Case when Retrieve = 'Yes' then SafeName end) as RetrieveSafes, IFNULL(Sum(Case When [Delete] ='Yes' then Accounts end),0) as DeleteAccounts, Count(Distinct Case when [Delete] ='Yes' then SafeName end) as DeleteOnSafes, IFNULL(Sum(Case when UpdateObjectProperties = 'Yes' then Accounts end),0) as UpdateAccountProperties, Count(Distinct Case when UpdateObjectProperties = 'Yes' then SafeName end) as UpdateObjectPropertiesSafes, IFNULL(Sum(Case when RenameObject = 'Yes' then Accounts end),0) as RenameAccountObject, Count(Distinct Case when RenameObject = 'Yes' then SafeName end) as RenameObjectSafes, IFNULL(Sum(Case When UnlockObject = 'Yes' then Accounts end),0) as UnlockAccounts, Count(Distinct Case when UnlockObject = 'Yes' then SafeName end) as UnlockObjectSafes, IFNULL(Sum(Case when UpdateObject = 'Yes' then Accounts end),0) as SetManualPasswordInVault, Count(Distinct Case when UpdateObject = 'Yes' then SafeName end) as UpdateObjectSafes, IFNULL(Sum(Case when InitiateCPMChange = 'Yes' then Accounts end),0) as InitiateCPMChange, Count(Distinct Case when InitiateCPMChange = 'Yes' then SafeName end) as InitiateCPMChangeSafes, IFNULL(Sum(Case when InitiateCPMChangeWithManualPassword = 'Yes' then Accounts end),0) as InitiateCPMChangeWithManualPassword, Count(Distinct Case when InitiateCPMChangeWithManualPassword = 'Yes' then SafeName end) as InitiateCPMChangeWithManualPasswordSafes, IFNULL(Sum(Case when Move = 'Yes' then Accounts end),0) as MoveAccountsAndFoldersInSafe, Count(Distinct Case when Move = 'Yes' then SafeName end) as MoveOnSafes, IFNULL(Sum(Case When confirm = 'Yes' then Accounts end),0) as ConfirmPermissionRequestsForAccordingSafe, Count(Distinct Case when confirm = 'Yes' then SafeName end) as ConfirmPermissionRequestsSafes, IFNULL(Sum(Case when NoConfirmRequired = 'Yes' then Accounts end),0) as AccessAccountWithoutRequiredConfirmation, Count(Distinct Case when NoConfirmRequired = 'Yes' then SafeName end) as NoConfirmRequiredSafes, IFNULL(Sum(case when [Backup] = 'Yes' then Accounts end),0) as BackupAccounts, Count(Distinct Case when [Backup] = 'Yes' then SafeName end) as BackupSafes, IFNULL(Sum(Case when ManageSafe = 'Yes' then Accounts end),0) as ManageSafeAccounts, Count(Distinct Case when ManageSafe = 'Yes' then SafeName end) as ManageSafe, IFNULL(Sum(Case when ManageSafeOwners = 'Yes' then Accounts end),0) as ManageSafeMembersAccounts, Count(Distinct Case when ManageSafeOwners = 'Yes' then SafeName end) as ManageSafeMembersSafes, IFNULL(Sum(Case when ViewOwners = 'Yes' then Accounts end),0) as ViewSafeMembersAccounts, Count(Distinct Case when ViewOwners = 'Yes' then SafeName end) as ViewSafeMembersSafes, IFNULL(Sum(Case when ViewAudit = 'Yes' then Accounts end),0) as ViewAuditAccounts, Count(Distinct Case when ViewAudit = 'Yes' then SafeName end) as ViewAuditSafes, IFNULL(Sum(Case when CreateFolder = 'Yes' then Accounts end),0) as CreateFolderAccounts, Count(Distinct Case when CreateFolder = 'Yes' then SafeName end) as CreateFolderSafes, IFNULL(Sum(Case when DeleteFolder = 'Yes' then Accounts end),0) as DeleteFolderAccounts, Count(Distinct Case when DeleteFolder = 'Yes' then SafeName end) as DeleteFolderSafes, IFNULL(Sum(Case when ValidateSafeContent = 'Yes' then Accounts end),0) as ValidateSafeContentAccounts, Count(Distinct Case when ValidateSafeContent = 'Yes' then SafeName end) as ValidateSafeContentSafes, IFNULL(Sum(Case when EventsList = 'Yes' then Accounts end),0) as EventsListAccounts, Count(Distinct Case when EventsList = 'Yes' then SafeName end) as EventsListSafes, IFNULL(Sum(Case when EventsAdd = 'Yes' then Accounts end),0) as EventsAddAccounts, Count(Distinct Case when EventsAdd = 'Yes' then SafeName end) as EventsAddSafes from (select OwnerShips.SafeName, OwnerShips.Accounts, Max(OwnerShips.List) as List, Max(OwnerShips.Retrieve) as Retrieve, Max(OwnerShips.UsePassword) as UsePassword, Max([Delete]) as [Delete], Max(OwnerShips.CreateObject) as CreateObject, Max(OwnerShips.UpdateObject) as UpdateObject, Max(OwnerShips.UpdateObjectProperties) as UpdateObjectProperties, Max(OwnerShips.RenameObject) as RenameObject, Max(OwnerShips.ViewAudit) as ViewAudit, Max(OwnerShips.ViewOwners) as ViewOwners, Max(OwnerShips.InitiateCPMChange) as InitiateCPMChange, Max(OwnerShips.InitiateCPMChangeWithManualPassword) as InitiateCPMChangeWithManualPassword, Max(OwnerShips.CreateFolder) as CreateFolder,Max(OwnerShips.DeleteFolder) as DeleteFolder, Max(OwnerShips.UnlockObject) as UnlockObject, Max(OwnerShips.Move) as Move, Max(OwnerShips.ManageSafe) as ManageSafe, Max(OwnerShips.ManageSafeOwners) as ManageSafeOwners, Max(OwnerShips.ValidateSafeContent) as ValidateSafeContent, Max(OwnerShips.[Backup]) as [Backup], Max(OwnerShips.NoConfirmRequired) as NoConfirmRequired, Max(OwnerShips.Confirm) as Confirm, Max(OwnerShips.EventsList) as EventsList, Max(OwnerShips.EventsAdd) as EventsAdd from OwnerShips where ownername in (select groupname from memberships where Member = $name union select $name) and [Empty / OLAC permission] = 'No' group by OwnerShips.SafeName)";
    
                    using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                    {
                        con.Open();
                        using (SQLiteCommand cmd = con.CreateCommand())
                        {

                            cmd.CommandText = @"PRAGMA temp_store = 2";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = @"Create temp table OwnerShips as select Owners.SafeName , case when a.Accounts is null then 0 else a.Accounts end as Accounts, Owners.OwnerName COLLATE NOCASE, Case Owners.OwnerName When 'Master' then 'Builtin' else (Case OwnerType When 0 then b.UserType else 'Group' end) end as 'UserType', Case When List = 'NO' and Retrieve = 'NO' and UsePassword = 'NO' and [Delete] = 'NO' and CreateObject = 'NO' and UpdateObject = 'NO' and UpdateObjectProperties = 'NO' and RenameObject = 'NO' and ViewAudit = 'NO' and ViewOwners = 'NO' and InitiateCPMChange = 'NO' and InitiateCPMChangeWithManualPassword = 'NO' and CreateFolder = 'NO' and DeleteFolder = 'NO' and UnlockObject = 'NO' and (MoveFrom = 'NO' and MoveInto = 'NO') and ManageSafe = 'NO' and ManageSafeOwners = 'NO' and ValidateSafeContent = 'NO' and Backup = 'NO' and NoConfirmRequired = 'NO' and Confirm = 'NO' and EventsList = 'NO' and EventsAdd = 'NO' Then 'Yes' Else 'No' END as [Empty / OLAC permission], Case When Owners.OwnerName in (select member from MemberShips) Then 'Yes' Else 'No' end as IsMemberOfGroups, Case When (OwnerType = 1 and Owners.OwnerName in (select groupname from MemberShips)) Then 'Yes' else 'No' end HasMembers, Case ExpirationDate When '' Then 'Never' When '0001-01-01 00:00:00' then 'Never' else ExpirationDate end as ExpirationDate, Case List when 'NO' then 'No' else 'Yes' end as List, Case Retrieve when 'NO' then 'No' else 'Yes' end as Retrieve, Case UsePassword when 'NO' then 'No' else 'Yes' end as UsePassword, Case [Delete] when 'NO' then 'No' else 'Yes' end as [Delete], Case CreateObject when 'NO' then 'No' else 'Yes' end as CreateObject, Case UpdateObject when 'NO' then 'No' else 'Yes' end as UpdateObject, Case UpdateObjectProperties when 'NO' then 'No' else 'Yes' end as UpdateObjectProperties, Case RenameObject when 'NO' then 'No' else 'Yes' end as RenameObject, Case ViewAudit when 'NO' then 'No' else 'Yes' end as ViewAudit, Case ViewOwners when 'NO' then 'No' else 'Yes' end as ViewOwners, Case InitiateCPMChange when 'NO' then 'No' else 'Yes' end as InitiateCPMChange, Case InitiateCPMChangeWithManualPassword when 'NO' then 'No' else 'Yes' end as InitiateCPMChangeWithManualPassword, Case CreateFolder when 'NO' then 'No' else 'Yes' end as CreateFolder, Case DeleteFolder when 'NO' then 'No' else 'Yes' end as DeleteFolder, Case UnlockObject when 'NO' then 'No' else 'Yes' end as UnlockObject, Case when MoveFrom = 'YES' or MoveInto = 'YES' then 'Yes' else 'No' end as Move, Case ManageSafe when 'NO' then 'No' else 'Yes' end as ManageSafe, Case ManageSafeOwners when 'NO' then 'No' else 'Yes' end as ManageSafeOwners, Case ValidateSafeContent when 'NO' then 'No' else 'Yes' end as ValidateSafeContent, Case [Backup] when 'NO' then 'No' else 'Yes' end as [Backup], Case NoConfirmRequired when 'NO' then 'No' else 'Yes' end as NoConfirmRequired, Case Confirm When 'NO' then 'No' else 'Yes' end as Confirm, Case EventsList when 'NO' then 'No' else 'Yes' end as EventsList, Case EventsAdd when 'NO' then 'No' else 'Yes' end as EventsAdd from owners left join (select Trim(accounts.safename) as safename, count(*) as accounts from accounts where DeletedBy = '' group by lower(safename)) a using (safename) left join (select UserName as OwnerName, UserType from TempUsers) b using (OwnerName)";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "Create INDEX if not exists i_OwnerAndOlac ON OwnerShips(OwnerName, [Empty / OLAC permission]);";
                            cmd.ExecuteNonQuery();

                            for (int i = 0; i < usersAndGroupsPermissions.Rows.Count; i++)
                            {
                                cmd.CommandText = queryString1;
                                da = new SQLiteDataAdapter(cmd);
                                tmpTable = new DataTable();
                                cmd.Parameters.AddWithValue("$name", usersAndGroupsPermissions.Rows[i]["Name"]);

                                if (Convert.ToInt32(cmd.ExecuteScalar()) > 0)
                                {
                                    usersAndGroupsPermissions.Rows[i]["Permissions"] = true;
                                    usersAndGroupsPermissions.Rows[i]["HasPermissions"] = "Yes";
                                    usersAndGroupsPermissions.Rows[i]["ButtonText"] = "View safes/accounts";

                                }
                                else
                                {
                                    usersAndGroupsPermissions.Rows[i]["Permissions"] = false;
                                    usersAndGroupsPermissions.Rows[i]["HasPermissions"] = "No";
                                    usersAndGroupsPermissions.Rows[i]["ButtonText"] = "Has no safes/accounts";
                                }

                                cmd.CommandText = queryString3;
                                tmpTable = new DataTable();
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(tmpTable);


                                usersAndGroupsPermissions.Rows[i]["ListRetrieveAccounts"] = tmpTable.Rows[0]["ListRetrieveAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveSafes"] = tmpTable.Rows[0]["ListRetrieveSafes"];

                                usersAndGroupsPermissions.Rows[i]["ListUseAccounts"] = tmpTable.Rows[0]["ListUseAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListUseSafes"] = tmpTable.Rows[0]["ListUseSafes"];

                                usersAndGroupsPermissions.Rows[i]["ListViewOwnersAccounts"] = tmpTable.Rows[0]["ListViewOwnersAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListViewOwnersSafes"] = tmpTable.Rows[0]["ListViewOwnersSafes"];

                                usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersAccounts"] = tmpTable.Rows[0]["ListRetrieveViewOwnersAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersSafes"] = tmpTable.Rows[0]["ListRetrieveViewOwnersSafes"];

                                usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmAccounts"] = tmpTable.Rows[0]["ListRetrieveNoConfirmAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmSafes"] = tmpTable.Rows[0]["ListRetrieveNoConfirmSafes"];

                                usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmAccounts"] = tmpTable.Rows[0]["ListUseNoConfirmAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmSafes"] = tmpTable.Rows[0]["ListUseNoConfirmSafes"];


                                usersAndGroupsPermissions.Rows[i]["ListAccounts"] = tmpTable.Rows[0]["ListAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ListSafes"] = tmpTable.Rows[0]["ListSafes"];
                                usersAndGroupsPermissions.Rows[i]["UseAccounts"] = tmpTable.Rows[0]["UseAccounts"];
                                usersAndGroupsPermissions.Rows[i]["UseSafes"] = tmpTable.Rows[0]["UseSafes"];
                                usersAndGroupsPermissions.Rows[i]["CreateAccounts"] = tmpTable.Rows[0]["CreateAccounts"];
                                usersAndGroupsPermissions.Rows[i]["CreateOnSafes"] = tmpTable.Rows[0]["CreateOnSafes"];
                                usersAndGroupsPermissions.Rows[i]["RetrievePasswords"] = tmpTable.Rows[0]["RetrievePasswords"];
                                usersAndGroupsPermissions.Rows[i]["RetrieveSafes"] = tmpTable.Rows[0]["RetrieveSafes"];
                                usersAndGroupsPermissions.Rows[i]["DeleteAccounts"] = tmpTable.Rows[0]["DeleteAccounts"];
                                usersAndGroupsPermissions.Rows[i]["DeleteSafes"] = tmpTable.Rows[0]["DeleteOnSafes"];
                                usersAndGroupsPermissions.Rows[i]["UpdateAccountProperties"] = tmpTable.Rows[0]["UpdateAccountProperties"];
                                usersAndGroupsPermissions.Rows[i]["UpdateObjectPropertiesSafes"] = tmpTable.Rows[0]["UpdateObjectPropertiesSafes"];
                                usersAndGroupsPermissions.Rows[i]["RenameAccountObject"] = tmpTable.Rows[0]["RenameAccountObject"];
                                usersAndGroupsPermissions.Rows[i]["RenameObjectSafes"] = tmpTable.Rows[0]["RenameObjectSafes"];
                                usersAndGroupsPermissions.Rows[i]["UnlockAccounts"] = tmpTable.Rows[0]["UnlockAccounts"];
                                usersAndGroupsPermissions.Rows[i]["UnlockObjectSafes"] = tmpTable.Rows[0]["UnlockObjectSafes"];
                                usersAndGroupsPermissions.Rows[i]["SetManualPasswordInVault"] = tmpTable.Rows[0]["SetManualPasswordInVault"];
                                usersAndGroupsPermissions.Rows[i]["UpdateObjectSafes"] = tmpTable.Rows[0]["UpdateObjectSafes"];
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChange"] = tmpTable.Rows[0]["InitiateCPMChange"];
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeSafes"] = tmpTable.Rows[0]["InitiateCPMChangeSafes"];
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeWithManualPassword"] = tmpTable.Rows[0]["InitiateCPMChangeWithManualPassword"];
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeWithManualPasswordSafes"] = tmpTable.Rows[0]["InitiateCPMChangeWithManualPasswordSafes"];
                                usersAndGroupsPermissions.Rows[i]["MoveAccountsAndFoldersInSafe"] = tmpTable.Rows[0]["MoveAccountsAndFoldersInSafe"];
                                usersAndGroupsPermissions.Rows[i]["MoveOnSafes"] = tmpTable.Rows[0]["MoveOnSafes"];
                                usersAndGroupsPermissions.Rows[i]["ConfirmPermissionRequestsForAccordingSafe"] = tmpTable.Rows[0]["ConfirmPermissionRequestsForAccordingSafe"];
                                usersAndGroupsPermissions.Rows[i]["ConfirmPermissionRequestsSafes"] = tmpTable.Rows[0]["ConfirmPermissionRequestsSafes"];
                                usersAndGroupsPermissions.Rows[i]["AccessAccountWithoutRequiredConfirmation"] = tmpTable.Rows[0]["AccessAccountWithoutRequiredConfirmation"];
                                usersAndGroupsPermissions.Rows[i]["NoConfirmRequiredSafes"] = tmpTable.Rows[0]["NoConfirmRequiredSafes"];
                                usersAndGroupsPermissions.Rows[i]["BackupAccounts"] = tmpTable.Rows[0]["BackupAccounts"];
                                usersAndGroupsPermissions.Rows[i]["BackupSafes"] = tmpTable.Rows[0]["BackupSafes"];
                                usersAndGroupsPermissions.Rows[i]["ManageSafeAccounts"] = tmpTable.Rows[0]["ManageSafeAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ManageSafe"] = tmpTable.Rows[0]["ManageSafe"];
                                usersAndGroupsPermissions.Rows[i]["ManageSafeMembersAccounts"] = tmpTable.Rows[0]["ManageSafeMembersAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ManageSafeMembersSafes"] = tmpTable.Rows[0]["ManageSafeMembersSafes"];
                                usersAndGroupsPermissions.Rows[i]["ViewSafeMembersAccounts"] = tmpTable.Rows[0]["ViewSafeMembersAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ViewSafeMembersSafes"] = tmpTable.Rows[0]["ViewSafeMembersSafes"];
                                usersAndGroupsPermissions.Rows[i]["ViewAuditAccounts"] = tmpTable.Rows[0]["ViewAuditAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ViewAuditSafes"] = tmpTable.Rows[0]["ViewAuditSafes"];
                                usersAndGroupsPermissions.Rows[i]["CreateFolderAccounts"] = tmpTable.Rows[0]["CreateFolderAccounts"];
                                usersAndGroupsPermissions.Rows[i]["CreateFolderSafes"] = tmpTable.Rows[0]["CreateFolderSafes"];
                                usersAndGroupsPermissions.Rows[i]["DeleteFolderAccounts"] = tmpTable.Rows[0]["DeleteFolderAccounts"];
                                usersAndGroupsPermissions.Rows[i]["DeleteFolderSafes"] = tmpTable.Rows[0]["DeleteFolderSafes"];
                                usersAndGroupsPermissions.Rows[i]["ValidateSafeContentAccounts"] = tmpTable.Rows[0]["ValidateSafeContentAccounts"];
                                usersAndGroupsPermissions.Rows[i]["ValidateSafeContentSafes"] = tmpTable.Rows[0]["ValidateSafeContentSafes"];
                                usersAndGroupsPermissions.Rows[i]["EventsListAccounts"] = tmpTable.Rows[0]["EventsListAccounts"];
                                usersAndGroupsPermissions.Rows[i]["EventsListSafes"] = tmpTable.Rows[0]["EventsListSafes"];
                                usersAndGroupsPermissions.Rows[i]["EventsAddAccounts"] = tmpTable.Rows[0]["EventsAddAccounts"];
                                usersAndGroupsPermissions.Rows[i]["EventsAddSafes"] = tmpTable.Rows[0]["EventsAddSafes"];



                                usersAndGroupsPermissions.Rows[i]["ListRetrieveAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListRetrieveAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListRetrieveSafes"]) * 100.00 / safesCount);

                                usersAndGroupsPermissions.Rows[i]["ListUseAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListUseAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListUseSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListUseSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ListViewOwnersAccounts(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListViewOwnersAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListViewOwnersSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListViewOwnersSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersAccounts(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersSafes(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListRetrieveViewOwnersSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmAccounts(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmSafes(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListRetrieveNoConfirmSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmAccounts(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmSafes(%)"] = roundNumber(Convert.ToInt32(usersAndGroupsPermissions.Rows[i]["ListUseNoConfirmSafes"]) * 100.00 / safesCount);



                                usersAndGroupsPermissions.Rows[i]["ListAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ListSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ListSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["CreateAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["CreateAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["CreateOnSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["CreateOnSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["UseAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UseAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["UseSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UseSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["RetrievePasswords(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["RetrievePasswords"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["RetrieveSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["RetrieveSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["DeleteAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["DeleteAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["DeleteSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["DeleteOnSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["UpdateAccountProperties(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UpdateAccountProperties"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["UpdateObjectPropertiesSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UpdateObjectPropertiesSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["RenameAccountObject(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["RenameAccountObject"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["RenameObjectSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["RenameObjectSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["UnlockAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UnlockAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["UnlockObjectSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UnlockObjectSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["SetManualPasswordInVault(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["SetManualPasswordInVault"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["UpdateObjectSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["UpdateObjectSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChange(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["InitiateCPMChange"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["InitiateCPMChangeSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeWithManualPassword(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["InitiateCPMChangeWithManualPassword"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["InitiateCPMChangeWithManualPasswordSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["InitiateCPMChangeWithManualPasswordSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["MoveAccountsAndFoldersInSafe(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["MoveAccountsAndFoldersInSafe"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["MoveOnSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["MoveOnSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ConfirmPermissionRequestsForAccordingSafe(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ConfirmPermissionRequestsForAccordingSafe"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ConfirmPermissionRequestsSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ConfirmPermissionRequestsSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["AccessAccountWithoutRequiredConfirmation(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["AccessAccountWithoutRequiredConfirmation"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["NoConfirmRequiredSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["NoConfirmRequiredSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["BackupAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["BackupAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["BackupSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["BackupSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ManageSafeAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ManageSafeAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ManageSafe(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ManageSafe"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ManageSafeMembersAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ManageSafeMembersAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ManageSafeMembersSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ManageSafeMembersSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ViewSafeMembersAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ViewSafeMembersAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ViewSafeMembersSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ViewSafeMembersSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ViewAuditAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ViewAuditAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ViewAuditSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ViewAuditSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["CreateFolderAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["CreateFolderAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["CreateFolderSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["CreateFolderSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["DeleteFolderAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["DeleteFolderAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["DeleteFolderSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["DeleteFolderSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["ValidateSafeContentAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ValidateSafeContentAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["ValidateSafeContentSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["ValidateSafeContentSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["EventsListAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["EventsListAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["EventsListSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["EventsListSafes"]) * 100.00 / safesCount);


                                usersAndGroupsPermissions.Rows[i]["EventsAddAccounts(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["EventsAddAccounts"]) * 100.00 / effectiveNumberOfAccounts);
                                usersAndGroupsPermissions.Rows[i]["EventsAddSafes(%)"] = roundNumber(Convert.ToInt32(tmpTable.Rows[0]["EventsAddSafes"]) * 100.00 / safesCount);
                            }

                            usersAndGroupsPermissions.DefaultView.Sort = "ListRetrieveAccounts DESC";
                            usersAndGroupsPermissions = usersAndGroupsPermissions.DefaultView.ToTable();

                            DBFunctions.storeDataTableInSqliteDatabase(usersAndGroupsPermissions);

                            if (DBFunctions.checkIfTableExistsOrIsEmpty("usersAndGroupsPermissions"))
                            {
                                cmd.CommandText = "Create index if not exists i_userType on usersAndGroupsPermissions(Type)";
                                cmd.ExecuteNonQuery();
                            }
                        }
                        con.Close();
                    }

                    tempDataTable = new DataTable();
                    queryString = "Create table OwnerShips as select Owners.SafeName , case when a.Accounts is null then 0 else a.Accounts end as Accounts, Owners.OwnerName COLLATE NOCASE, Case Owners.OwnerName When 'Master' then 'Builtin' else (Case OwnerType When 0 then b.UserType else 'Group' end) end as 'UserType', Case When List = 'NO' and Retrieve = 'NO' and UsePassword = 'NO' and [Delete] = 'NO' and CreateObject = 'NO' and UpdateObject = 'NO' and UpdateObjectProperties = 'NO' and RenameObject = 'NO' and ViewAudit = 'NO' and ViewOwners = 'NO' and InitiateCPMChange = 'NO' and InitiateCPMChangeWithManualPassword = 'NO' and CreateFolder = 'NO' and DeleteFolder = 'NO' and UnlockObject = 'NO' and (MoveFrom = 'NO' and MoveInto = 'NO') and ManageSafe = 'NO' and ManageSafeOwners = 'NO' and ValidateSafeContent = 'NO' and Backup = 'NO' and NoConfirmRequired = 'NO' and Confirm = 'NO' and EventsList = 'NO' and EventsAdd = 'NO' Then 'Yes' Else 'No' END as [Empty / OLAC permission], Case When Owners.OwnerName in (select member from MemberShips) Then 'Yes' Else 'No' end as IsMemberOfGroups, Case When (OwnerType = 1 and Owners.OwnerName in (select groupname from MemberShips)) Then 'Yes' else 'No' end HasMembers, Case ExpirationDate When '' Then 'Never' When '0001-01-01 00:00:00' then 'Never' else ExpirationDate end as ExpirationDate, Case List when 'NO' then 'No' else 'Yes' end as List, Case Retrieve when 'NO' then 'No' else 'Yes' end as Retrieve, Case UsePassword when 'NO' then 'No' else 'Yes' end as UsePassword, Case [Delete] when 'NO' then 'No' else 'Yes' end as [Delete], Case CreateObject when 'NO' then 'No' else 'Yes' end as CreateObject, Case UpdateObject when 'NO' then 'No' else 'Yes' end as UpdateObject, Case UpdateObjectProperties when 'NO' then 'No' else 'Yes' end as UpdateObjectProperties, Case RenameObject when 'NO' then 'No' else 'Yes' end as RenameObject, Case ViewAudit when 'NO' then 'No' else 'Yes' end as ViewAudit, Case ViewOwners when 'NO' then 'No' else 'Yes' end as ViewOwners, Case InitiateCPMChange when 'NO' then 'No' else 'Yes' end as InitiateCPMChange, Case InitiateCPMChangeWithManualPassword when 'NO' then 'No' else 'Yes' end as InitiateCPMChangeWithManualPassword, Case CreateFolder when 'NO' then 'No' else 'Yes' end as CreateFolder, Case DeleteFolder when 'NO' then 'No' else 'Yes' end as DeleteFolder, Case UnlockObject when 'NO' then 'No' else 'Yes' end as UnlockObject, Case when MoveFrom = 'YES' or MoveInto = 'YES' then 'Yes' else 'No' end as Move, Case ManageSafe when 'NO' then 'No' else 'Yes' end as ManageSafe, Case ManageSafeOwners when 'NO' then 'No' else 'Yes' end as ManageSafeOwners, Case ValidateSafeContent when 'NO' then 'No' else 'Yes' end as ValidateSafeContent, Case [Backup] when 'NO' then 'No' else 'Yes' end as [Backup], Case NoConfirmRequired when 'NO' then 'No' else 'Yes' end as NoConfirmRequired, Case Confirm When 'NO' then 'No' else 'Yes' end as Confirm, Case EventsList when 'NO' then 'No' else 'Yes' end as EventsList, Case EventsAdd when 'NO' then 'No' else 'Yes' end as EventsAdd from owners left join (select Trim(accounts.safename) as safename, count(*) as accounts from accounts where DeletedBy = '' group by lower(safename)) a using (safename) left join (select UserName as OwnerName, UserType from TempUsers) b using (OwnerName)";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    command.ExecuteNonQuery();
                    DBFunctions.closeDBConnection();

                    queryString = "select * from OwnerShips";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(ownerShips);
                    DBFunctions.closeDBConnection();

                    if (DBFunctions.checkIfTableExistsOrIsEmpty("usersAndGroupsPermissions"))
                    {
                        queryString = "select * from (select [List accounts access on % of all accounts], [EPV Users] from (select count(Name) as [EPV Users], 'Between 0% and 10% of all accounts' as [List accounts access on % of all accounts] from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] >= 0 and [ListAccounts(%)] <= 10 union select count(Name) as Users, 'Between 10% and 20% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 10 and [ListAccounts(%)] <= 20 union select count(Name) as Users, 'Between 20% and 30% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 20 and [ListAccounts(%)] <=30 union select count(Name) as Users, 'Between 30% and 40% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 30 and [ListAccounts(%)] <=40 union select count(Name) as Users, 'Between 40% and 50% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 40 and [ListAccounts(%)] <= 50 union select count(Name) as Users, 'Between 50% and 60% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 50 and [ListAccounts(%)] <=60 union select count(Name) as Users, 'Between 60% and 70% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 60 and [ListAccounts(%)] <= 70 union select count(Name) as Users, 'Between 70% and 80% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 70 and [ListAccounts(%)] <= 80 union select count(Name) as Users, 'Between 80% and 90% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 80 and [ListAccounts(%)] <=90  union select count(Name) as Users, 'Between 90% and 100% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListAccounts(%)] > 90 and [ListAccounts(%)] <= 100) order by [List accounts access on % of all accounts]) where [EPV Users] > 0";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(epvUsersWithListAccess);
                        DBFunctions.closeDBConnection();

                        queryString = "select * from (select [List + retrieve password access on % of all accounts], [EPV Users] from (select count(Name) as [EPV Users], 'Between 0% and 10% of all accounts' as [List + retrieve password access on % of all accounts] from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] >= 0 and [ListRetrieveAccounts(%)] <= 10 union select count(Name) as Users, 'Between 10% and 20% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 10 and [ListRetrieveAccounts(%)] <= 20 union select count(Name) as Users, 'Between 20% and 30% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 20 and [ListRetrieveAccounts(%)] <=30 union select count(Name) as Users, 'Between 30% and 40% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 30 and [ListRetrieveAccounts(%)] <=40 union select count(Name) as Users, 'Between 40% and 50% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 40 and [ListRetrieveAccounts(%)] <= 50 union select count(Name) as Users, 'Between 50% and 60% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 50 and [ListRetrieveAccounts(%)] <=60 union select count(Name) as Users, 'Between 60% and 70% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 60 and [ListRetrieveAccounts(%)] <= 70 union select count(Name) as Users, 'Between 70% and 80% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 70 and [ListRetrieveAccounts(%)] <= 80 union select count(Name) as Users, 'Between 80% and 90% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 80 and [ListRetrieveAccounts(%)] <=90  union select count(Name) as Users, 'Between 90% and 100% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveAccounts(%)] > 90 and [ListRetrieveAccounts(%)] <= 100) order by [List + retrieve password access on % of all accounts]) where [EPV Users] > 0";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(epvUsersWithListRetrieveAccess);
                        DBFunctions.closeDBConnection();

                        queryString = "select * from (select [List + retrieve + w/o confirmation access on % of all accounts], [EPV Users] from (select count(Name) as [EPV Users], 'Between 0% and 10% of all accounts' as [List + retrieve + w/o confirmation access on % of all accounts] from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] >= 0 and [ListRetrieveNoConfirmAccounts(%)] <= 10 union select count(Name) as Users, 'Between 10% and 20% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 10 and [ListRetrieveNoConfirmAccounts(%)] <= 20 union select count(Name) as Users, 'Between 20% and 30% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 20 and [ListRetrieveNoConfirmAccounts(%)] <=30 union select count(Name) as Users, 'Between 30% and 40% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 30 and [ListRetrieveNoConfirmAccounts(%)] <=40 union select count(Name) as Users, 'Between 40% and 50% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 40 and [ListRetrieveNoConfirmAccounts(%)] <= 50 union select count(Name) as Users, 'Between 50% and 60% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 50 and [ListRetrieveNoConfirmAccounts(%)] <=60 union select count(Name) as Users, 'Between 60% and 70% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 60 and [ListRetrieveNoConfirmAccounts(%)] <= 70 union select count(Name) as Users, 'Between 70% and 80% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 70 and [ListRetrieveNoConfirmAccounts(%)] <= 80 union select count(Name) as Users, 'Between 80% and 90% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 80 and [ListRetrieveNoConfirmAccounts(%)] <=90  union select count(Name) as Users, 'Between 90% and 100% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListRetrieveNoConfirmAccounts(%)] > 90 and [ListRetrieveNoConfirmAccounts(%)] <= 100) order by [List + retrieve + w/o confirmation access on % of all accounts]) where [EPV Users] > 0";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(epvUsersWithListRetrieveNoConfirmAccess);
                        DBFunctions.closeDBConnection();

                        queryString = "select * from (select [List + use password on % of all accounts], [EPV Users] from (select count(Name) as [EPV Users], 'Between 0% and 10% of all accounts' as [List + use password on % of all accounts] from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] >= 0 and [ListUseAccounts(%)] <= 10 union select count(Name) as Users, 'Between 10% and 20% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 10 and [ListUseAccounts(%)] <= 20 union select count(Name) as Users, 'Between 20% and 30% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 20 and [ListUseAccounts(%)] <=30 union select count(Name) as Users, 'Between 30% and 40% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 30 and [ListUseAccounts(%)] <=40 union select count(Name) as Users, 'Between 40% and 50% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 40 and [ListUseAccounts(%)] <= 50 union select count(Name) as Users, 'Between 50% and 60% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 50 and [ListUseAccounts(%)] <=60 union select count(Name) as Users, 'Between 60% and 70% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 60 and [ListUseAccounts(%)] <= 70 union select count(Name) as Users, 'Between 70% and 80% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 70 and [ListUseAccounts(%)] <= 80 union select count(Name) as Users, 'Between 80% and 90% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 80 and [ListUseAccounts(%)] <=90  union select count(Name) as Users, 'Between 90% and 100% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseAccounts(%)] > 90 and [ListUseAccounts(%)] <= 100) order by [List + use password on % of all accounts]) where [EPV Users] > 0";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(epvUsersWithListUseAccess);
                        DBFunctions.closeDBConnection();

                        queryString = "select * from (select [List + use + w/o confirmation on % of all accounts], [EPV Users] from (select count(Name) as [EPV Users], 'Between 0% and 10% of all accounts' as [List + use + w/o confirmation on % of all accounts] from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] >= 0 and [ListUseNoConfirmAccounts(%)] <= 10 union select count(Name) as Users, 'Between 10% and 20% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 10 and [ListUseNoConfirmAccounts(%)] <= 20 union select count(Name) as Users, 'Between 20% and 30% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 20 and [ListUseNoConfirmAccounts(%)] <=30 union select count(Name) as Users, 'Between 30% and 40% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 30 and [ListUseNoConfirmAccounts(%)] <=40 union select count(Name) as Users, 'Between 40% and 50% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 40 and [ListUseNoConfirmAccounts(%)] <= 50 union select count(Name) as Users, 'Between 50% and 60% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 50 and [ListUseNoConfirmAccounts(%)] <=60 union select count(Name) as Users, 'Between 60% and 70% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 60 and [ListUseNoConfirmAccounts(%)] <= 70 union select count(Name) as Users, 'Between 70% and 80% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 70 and [ListUseNoConfirmAccounts(%)] <= 80 union select count(Name) as Users, 'Between 80% and 90% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 80 and [ListUseNoConfirmAccounts(%)] <=90  union select count(Name) as Users, 'Between 90% and 100% of all accounts' Description from UsersAndGroupsPermissions where (Type = 'EPV' or Name = 'Administrator') and [ListUseNoConfirmAccounts(%)] > 90 and [ListUseNoConfirmAccounts(%)] <= 100) order by [List + use + w/o confirmation on % of all accounts]) where [EPV Users] > 0";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(epvUsersWithListUseNoConfirmAccess);
                        DBFunctions.closeDBConnection();

                        queryString = "select Name as [AAM Provider], Disabled, ListRetrieveViewOwnersAccounts as Accounts, [ListRetrieveViewOwnersAccounts(%)] as [Accounts (%)], ListRetrieveViewOwnersSafes as Safes, [ListRetrieveViewOwnersSafes(%)] as [Safes (%)] from UsersAndGroupsPermissions where (Type = 'AAM Provider') order by [Accounts] desc, Name";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(aamProviderPermissions);
                        DBFunctions.closeDBConnection();


                        queryString = "select Name as [AAM Application], Disabled, ListRetrieveAccounts as Accounts, [ListRetrieveAccounts(%)] as [Accounts (%)], ListRetrieveSafes as Safes, [ListRetrieveSafes(%)] as [Safes (%)] from UsersAndGroupsPermissions where (Type = 'AAM Application') order by [Accounts] desc, Name";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(aamApplicationPermissions);
                        DBFunctions.closeDBConnection();

                        queryString = "Select Distinct(SafeName) as [Possible OLAC Safes], Accounts from OwnerShips Where[Empty / OLAC Permission] = 'Yes'";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(safesWithOLAC);
                        DBFunctions.closeDBConnection();
                        olacSafes = safesWithOLAC.Rows.Count;

                        int averageEPVusersListAccounts = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListAccounts])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListAccounts = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListSafes = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListSafes])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListRetrieveAccounts = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveAccounts])) As Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListRetrieveAccounts = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListRetrieveSafes = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveSafes])) As Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListRetrieveSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListRetrieveNoConfirmAccounts = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveNoConfirmAccounts])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListRetrieveNoConfirmAccounts = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListRetrieveNoConfirmSafes = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveNoConfirmSafes])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListRetrieveNoConfirmSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int accountsInPossibleOlacSafes = 0;
                        queryString = "SELECT IFNULL(Accounts,0) from (select sum(accounts) as Accounts from (Select Distinct(SafeName) as [OLAC Safes], Accounts  from OwnerShips Where [Empty / OLAC Permission] = 'Yes'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        accountsInPossibleOlacSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();

                        int averageEPVusersListUseAccounts = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListUseAccounts])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListUseAccounts = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListUseSafes = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListUseSafes])) As Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListUseSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListUseNoConfirmAccounts = 0;
                        queryString = "Select IFNULL(Average, 0) FROM (select Round(avg([ListUseNoConfirmAccounts])) as Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListUseNoConfirmAccounts = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageEPVusersListUseNoConfirmSafes = 0;
                        queryString = "select IFNULL(Average, 0) from (select Round(avg([ListUseNoConfirmSafes])) As Average from usersandgroupspermissions where (type = 'EPV' or name = 'Administrator'))";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        averageEPVusersListUseNoConfirmSafes = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();


                        int averageAAMProviderAccessOnAccounts = 0;
                        int averageAAMApplicationAccessOnAccounts = 0;
                        int averageOPMAgentAccessOnAccounts = 0;
                        int averageAAMProviderAccessOnSafes = 0;
                        int averageAAMApplicationAccessOnSafes = 0;
                        int averageOPMAgentAccessOnSafes = 0;


                        if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("AAM Application")).ToList().Count > 0)
                        {
                            queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveViewOwnersAccounts])) as Average from usersandgroupspermissions where type = 'AAM Application')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageAAMApplicationAccessOnAccounts = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();

                            queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveViewOwnersSafes])) as Average from usersandgroupspermissions where type = 'AAM Application')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageAAMApplicationAccessOnSafes = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();
                        }

                        if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("AAM Provider")).ToList().Count > 0)
                        {
                            queryString = "select IFNULL(Average, 0) from (select Round(avg([ListRetrieveViewOwnersAccounts])) as Average from usersandgroupspermissions where type = 'AAM Provider')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageAAMProviderAccessOnAccounts = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();

                            queryString = "select IFNULL(Average, 0)  from (select Round(avg([ListRetrieveViewOwnersSafes])) as Average from usersandgroupspermissions where type = 'AAM Provider')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageAAMProviderAccessOnSafes = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();
                        }

                        if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("OPM Agent")).ToList().Count > 0)
                        {
                            queryString = "select IFNULL(Average, 0) from (select Round(avg([ListViewOwnersAccounts])) as Average from usersandgroupspermissions where type = 'OPM Agent')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageOPMAgentAccessOnAccounts = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();

                            queryString = "select IFNULL(Average, 0) from (select Round(avg([ListViewOwnersSafes])) as Average from usersandgroupspermissions where type = 'OPM Agent')";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            averageOPMAgentAccessOnSafes = Convert.ToInt32(command.ExecuteScalar());
                            DBFunctions.closeDBConnection();

                            queryString = "select Name as [OPM Agent], Disabled, [ListViewOwnersAccounts] as Accounts, [ListViewOwnersAccounts(%)] as [Accounts (%)], [ListViewOwnersSafes] as Safes, [ListViewOwnersSafes(%)] as [Safes (%)]  from usersandgroupspermissions where type = 'OPM Agent' order by Accounts desc, Name";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(opmAgentPermissions);
                            DBFunctions.closeDBConnection();
                        }



                        permissionStatistics.Columns.Add("Description", typeof(string));
                        permissionStatistics.Columns.Add("Statistic", typeof(string));
                        permissionStatistics.Columns.Add("Comment", typeof(string));

                        permissionStatistics.Rows.Add("Total number of accounts", string.Format("{0:#,##0}", effectiveNumberOfAccounts));
                        permissionStatistics.Rows.Add("Total number of safes", string.Format("{0:#,##0}", numberOfSafes));
                        permissionStatistics.Rows.Add("EPV users", string.Format("{0:#,##0}", usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("EPV")).ToList().Count));


                        permissionStatistics.Rows.Add("Safes with OLAC", string.Format("{0:#,##0}", olacSafes), "Note: This is just an indication of safes that might have OLAC enabled since OLAC safes cannot get determined with absolute certainty with EVD data");
                        permissionStatistics.Rows.Add("Accounts in possible OLAC safes", string.Format("{0:#,##0}", accountsInPossibleOlacSafes), Math.Round((accountsInPossibleOlacSafes * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts | Note: OLAC safes cannot get determined with absolute certainty with EVD data");

                        permissionStatistics.Rows.Add("Average EPV users list access on accounts", string.Format("{0:#,##0}", averageEPVusersListAccounts), Math.Round((averageEPVusersListAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");

                        permissionStatistics.Rows.Add("Average EPV users list + retrieve password access on accounts", string.Format("{0:#,##0}", averageEPVusersListRetrieveAccounts), Math.Round((averageEPVusersListRetrieveAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");


                        permissionStatistics.Rows.Add("Average EPV users list + retrieve password + w/o confirmation access on accounts", string.Format("{0:#,##0}", averageEPVusersListRetrieveNoConfirmAccounts), Math.Round((averageEPVusersListRetrieveNoConfirmAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");

                        permissionStatistics.Rows.Add("Average EPV users list + use password on accounts", string.Format("{0:#,##0}", averageEPVusersListUseAccounts), Math.Round((averageEPVusersListUseAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");

                        permissionStatistics.Rows.Add("Average EPV users list + use password + w/o confirmation access on accounts", string.Format("{0:#,##0}", averageEPVusersListUseNoConfirmAccounts), Math.Round((averageEPVusersListUseNoConfirmAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");


                        permissionStatistics.Rows.Add("Average AAM Providers access on accounts", string.Format("{0:#,##0}", averageAAMProviderAccessOnAccounts), Math.Round((averageAAMProviderAccessOnAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");
                        permissionStatistics.Rows.Add("Average AAM Providers access on safes", string.Format("{0:#,##0}", averageAAMProviderAccessOnSafes), Math.Round((averageAAMProviderAccessOnSafes * 100.00 / numberOfSafes), 2) + "% of all safes");

                        permissionStatistics.Rows.Add("Average AAM Applications access on accounts", string.Format("{0:#,##0}", averageAAMProviderAccessOnAccounts), Math.Round((averageAAMProviderAccessOnAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");
                        permissionStatistics.Rows.Add("Average AAM Applications access on safes", string.Format("{0:#,##0}", averageAAMProviderAccessOnSafes), Math.Round((averageAAMProviderAccessOnSafes * 100.00 / numberOfSafes), 2) + "% of all safes");

                        permissionStatistics.Rows.Add("Average OPM Agents access on accounts", string.Format("{0:#,##0}", averageOPMAgentAccessOnAccounts), Math.Round((averageOPMAgentAccessOnAccounts * 100.00 / effectiveNumberOfAccounts), 2) + "% of all accounts");
                        permissionStatistics.Rows.Add("Average OPM Agents access on safes", string.Format("{0:#,##0}", averageOPMAgentAccessOnSafes), Math.Round((averageOPMAgentAccessOnSafes * 100.00 / numberOfSafes), 2) + "% of all safes");
                    }



                }

                installedVersions = await determineInstalledVersionsAsync();

                if (hasMasterPolicy && hasPlatformPolicies)
                {
                    masterPolicyTable.Columns.Add("HasException", typeof(string)).SetOrdinal(1);

                    bool hasAccountsPerPolicy = false;

                    if (accountsPerPolicyDataTable.Rows.Count > 0)
                    {
                        masterPolicyTable.Columns.Add("Accounts", typeof(int)).SetOrdinal(2);
                        hasAccountsPerPolicy = true;
                    }


                    for (int i = 0; i < platformPoliciesTable.Rows.Count; i++)
                    {
                        if (masterPolicyTable.AsEnumerable().Where(x => x.Field<string>("PolicyName").ToString().TrimStart().TrimEnd() == platformPoliciesTable.Rows[i]["PolicyName"].ToString().TrimStart().TrimEnd()).FirstOrDefault() == null)
                        {
                            masterPolicyTable.Rows.Add(masterPolicyTable.Rows[0].ItemArray.Clone() as object[]);
                            masterPolicyTable.Rows[masterPolicyTable.Rows.Count - 1]["PolicyName"] = platformPoliciesTable.Rows[i]["PolicyName"].ToString();
                            masterPolicyTable.Rows[masterPolicyTable.Rows.Count - 1]["HasException"] = "No";
                        }

                        for (int j = 1; j < masterPolicyTable.Rows.Count; j++)
                        {

                            if (masterPolicyTable.Rows[j]["HasException"].ToString() == "")
                            {
                                masterPolicyTable.Rows[j]["HasException"] = "Yes";
                            }

                            if (hasAccountsPerPolicy)
                            {
                                if (masterPolicyTable.Rows[j]["PolicyName"].ToString() != "Master Policy" && masterPolicyTable.Rows[j]["PolicyName"].ToString() == platformPoliciesTable.Rows[i]["PolicyName"].ToString())
                                {
                                    masterPolicyTable.Rows[j]["Accounts"] = platformPoliciesTable.Rows[i]["Accounts"];
                                    break;
                                }
                                if (masterPolicyTable.Rows[j]["PolicyName"].ToString().Trim() == "MasterPolicy")
                                {
                                    masterPolicyTable.Rows[j]["Accounts"] = DBNull.Value;
                                    break;
                                }
                            }
                        }
                    }
                }



                if (hasLogs)
                {

                    DateTime logsStartDate = new DateTime();
                    DateTime logsEndDate = new DateTime();

                    using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                    {
                        con.Open();

                        using (SQLiteCommand cmd = con.CreateCommand())
                        {
                            cmd.CommandText = @"select min(time) from logs limit 1000";
                            logsStartDate = DateTime.Parse(cmd.ExecuteScalar().ToString());

                            cmd.CommandText = @"select max(time) from logs order by rowID desc limit 1000";
                            logsEndDate = DateTime.Parse(cmd.ExecuteScalar().ToString());
                        }

                        con.Close();
                    }


                    totalLogsDays = (int)(logsEndDate - logsStartDate).TotalDays;
                    if (totalLogsDays == 0)
                    {
                        totalLogsDays = 1;
                    }

                    SQLiteCommand command;
                    SQLiteDataAdapter da;

                    // Determining sessions information...

                    DBFunctions.createViews();
                    DataSet tempDataSet = new DataSet();
                    DataTable tempDataTable = new DataTable();

                    tempDataTable = new DataTable();
                    queryString = "Create Table Sessions as select * from v_psmSessions where endtime is not null and starttime is not null group by starttime, endtime, erroroccurred, targetaccount union select * from v_adhocSessions where endtime is not null and starttime is not null group by starttime, endtime, erroroccurred, targetaccount;";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(tempDataTable);
                    DBFunctions.closeDBConnection();

                    if (hasRecordings)
                    {
                        using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                        {
                            con.Open();

                            using (SQLiteCommand cmd = con.CreateCommand())
                            {

                                using (SQLiteTransaction tr = con.BeginTransaction())
                                {
                                    cmd.Transaction = tr;

                                    cmd.CommandText = @"PRAGMA temp_store = 2";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = @"create temp table psmIDmappingTable as select * from (select CreatedBy, [Solution Server ID] from (select CreatedBy, a.[Solution Server ID] from sessionRecordings left join (select SessionID, [Solution Server ID] from sessions where [PSM Solution] = 'PSM' and RecordingFound = 'Yes') a on sessionRecordings.SessionID = a.SessionID) where [Solution Server ID] is not null group by CreatedBy, [Solution Server ID]) group by CreatedBy";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = @"create temp table safeNameMappingTable as select * from (select sessionRecordings.SessionID, a.AccountName, a.SafeName from sessionRecordings left join (select accounts.AccountName, accounts.SafeName, accounts.Address, accounts.UserName, accounts.PolicyID from accounts) a on sessionRecordings.TargetHost = a.Address and sessionRecordings.TargetUser = a.UserName and sessionRecordings.PolicyID = a.PolicyID where sessionRecordings.SessionID not in (select sessionID from sessions) ) where AccountName is not null";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = @"create index i_sessionID on  safeNameMappingTable(sessionID)";
                                    cmd.ExecuteNonQuery();

                                    cmd.CommandText = @"create index i_recordingsSessionID on  sessionRecordings(sessionID)";
                                    cmd.ExecuteNonQuery();

                                    cmd.Parameters.AddWithValue("$days", totalLogsDays);
                                    cmd.CommandText = @"insert into Sessions select [CyberArk User], Case When Solution = 'PSM' then (select [Solution Server ID] from psmIDmappingTable where psmIDmappingTable.CreatedBy = sessionRecordings.Createdby) else 'PSMServer' end as [Solution Server ID], Solution, (Select SafeName from safeNameMappingTable where safeNameMappingTable.SessionID = sessionRecordings.SessionID) As SafeName, PolicyID, DeviceType, (Select AccountName from safeNameMappingTable where safeNameMappingTable.SessionID = sessionRecordings.SessionID) as TargetAccount, TargetHost, TargetUser, ConnectionComponent, [Database], Case When PolicyID NOT Like '%PSMSecureConnect%' AND DeviceType Not Like '%PSM Secure Connect%' Then 'No' Else 'Yes' End as [Ad-Hoc Connection], PSMStartTime as StartTime, PSMEndTime as EndTime, 'Yes' as RecordingFound, 'No' as ConnectionFailed, 'No' as ErrorOccurred, 'No' as DurationElapsed, Null as Message, SessionID from sessionRecordings where PSMStartTime is not null and PSMEndTime is not null and [Age (days)] <= $days and sessionRecordings.SessionID not in (select sessionID from sessions) and (CreatedBy in (select CreatedBy from psmIDmappingTable) or Solution = 'PSM for SSH')";
                                    cmd.ExecuteNonQuery();

                                    tr.Commit();
                                }
                            }

                            con.Close();
                        }
                    }


                    queryString = "select * from Sessions group by starttime, endtime, erroroccurred, targetaccount;";
                    command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    da = new SQLiteDataAdapter(command);
                    da.Fill(sessionsDataTable);
                    DBFunctions.closeDBConnection();

                    opmSessions.Columns.Add("CyberArk User", typeof(string));
                    opmSessions.Columns.Add("AccountName", typeof(string));
                    opmSessions.Columns.Add("TargetHost", typeof(string));
                    opmSessions.Columns.Add("TargetUser", typeof(string));
                    opmSessions.Columns.Add("SafeName", typeof(string));
                    opmSessions.Columns.Add("Command", typeof(string));
                    opmSessions.Columns.Add("WorkingDirectory", typeof(string));
                    opmSessions.Columns.Add("StartTime", typeof(DateTime));
                    opmSessions.Columns.Add("EndTime", typeof(DateTime));
                    opmSessions.Columns.Add("CommandDuration", typeof(string));
                    opmSessions.Columns.Add("CommandFailed", typeof(string));
                    opmSessions.Columns.Add("ErrorOccurred", typeof(string));
                    opmSessions.Columns.Add("DurationElapsed", typeof(string));
                    opmSessions.Columns.Add("ErrorCode", typeof(string));
                    opmSessions.Columns.Add("Message", typeof(string));
                    opmSessions.Columns.Add("SessionID", typeof(string));

                    DataTable orderedOpmSessions = new DataTable();

                    try
                    {

                        using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\PASReporter.db; Version = 3;"))
                        {
                            con.Open();

                            using (SQLiteCommand cmd = con.CreateCommand())
                            {
                                cmd.CommandText = "Create TEMPORARY Table opmSessions as SELECT UserName AS [CyberArk User], AccountName, Address AS TargetHost, TargetUser, SafeName, Command, WorkingDirectory, CASE WHEN Code = 345 THEN Time ELSE DateTime(Time, '-' || (strftime('%S', CommandDuration) + (strftime('%M', CommandDuration) * 60) + (strftime('%H', CommandDuration) * 3600) ) || ' seconds') END AS StartTime, Time AS EndTime, CASE WHEN Code != 345 THEN CommandDuration ELSE '00:00:00' END AS CommandDuration, CommandFailed, ErrorOccurred, CASE WHEN ErrorMessage LIKE '%OPMSU414E%' THEN 'Yes' ELSE 'No' END AS DurationElapsed, ErrorCode, Trim([Replace]([Replace](ErrorMessage, '[' || sessionID || ']', ''), ErrorCode, '') ) AS Message, SessionID FROM ( SELECT CASE WHEN Code = 345 THEN 'Yes' ELSE 'No' END AS CommandFailed, CASE WHEN Code = 347 THEN 'Yes' ELSE 'No' END AS ErrorOccurred, Case WHEN RequestReason like '%OPMS%' Then substr(RequestReason, instr(RequestReason, 'OPMS'), 10) Else Null End AS ErrorCode, RequestReason AS ErrorMessage, [Replace](Info1, 'Root\\', '') AS AccountName, CASE WHEN Code NOT IN (346, 347) THEN substr(info2, 9, instr(info2, ';CWD') - 9) ELSE substr(info2, 9, instr(info2, ';Command') - 9) END AS Command, substr(info2, instr(info2, ';CWD') + 5, instr(info2, ';Host') - instr(info2, ';CWD') - 5) AS WorkingDirectory, substr(info2, instr(info2, ';Host') + 10, instr(info2, ';IP') - instr(info2, ';Host') - 10) AS TargetHost, substr(info2, instr(info2, ';IP') + 4, instr(info2, ';Privileged') - instr(info2, ';IP') - 4) AS Address, CASE WHEN Code = 345 THEN substr(info2, instr(info2, ';Privileged') + 16, instr(info2, ';UUID') - instr(info2, ';Privileged') - 16) ELSE substr(info2, instr(info2, ';Privileged') + 16, instr(info2, ';RC') - instr(info2, ';Privileged') - 16) END AS TargetUser, CASE WHEN Code NOT IN (346, 347) THEN [Replace](substr(info2, instr(info2, ';UUID') + 6, length(info2) ), ';', '') ELSE substr(info2, instr(info2, ';UUID') + 6, instr(info2, ';VID') - instr(info2, ';UUID') - 6) END AS SessionID, CASE WHEN Code NOT IN (346, 347) THEN NULL ELSE substr(info2, instr(info2, ';Command') + 17, instr(info2, ';CWD') - instr(info2, ';Command') - 17) END AS CommandDuration, * FROM logs WHERE code IN (345, 346, 347) ) ORDER BY StartTime;";
                                cmd.ExecuteScalar();
                                cmd.CommandText = "Select * from opmSessions;";
                                da = new SQLiteDataAdapter(cmd);
                                da.Fill(opmSessions);

                                if (isNotNullOrEmpty(opmSessions))
                                {
                                    cmd.CommandText = "Select * From (select 1 as Code, StartTime as Time, SessionID from opmSessions where CommandDuration != '00:00:00' union all Select 2 as Code, EndTime as Time, SessionID from opmSessions where CommandDuration != '00:00:00') order By Time;";
                                    da = new SQLiteDataAdapter(cmd);
                                    da.Fill(orderedOpmSessions);
                                }

                            }
                            con.Close();
                        }

                        if (isNotNullOrEmpty(orderedOpmSessions))
                        {
                            concurrentOpmSessions = new DataTable();
                            concurrentOpmSessions.Columns.Add("Time", typeof(DateTime));
                            concurrentOpmSessions.Columns.Add("ConcurrentSessions", typeof(int));
                            Dictionary<string, string> sessionIdDictionary = new Dictionary<string, string>();

                            for (int i = 0; i < orderedOpmSessions.Rows.Count; i++)
                            {
                                if (Convert.ToInt32(orderedOpmSessions.Rows[i]["Code"]) == 1 && !sessionIdDictionary.ContainsKey((string)orderedOpmSessions.Rows[i]["SessionID"]))
                                {
                                    sessionIdDictionary.Add((string)orderedOpmSessions.Rows[i]["SessionID"], (string)orderedOpmSessions.Rows[i]["SessionID"]);
                                    concurrentOpmSessions.Rows.Add(orderedOpmSessions.Rows[i]["Time"], sessionIdDictionary.Count);
                                }
                                else if (Convert.ToInt32(orderedOpmSessions.Rows[i]["Code"]) == 2 && sessionIdDictionary.ContainsKey((string)orderedOpmSessions.Rows[i]["SessionID"]))
                                {
                                    sessionIdDictionary.Remove((string)orderedOpmSessions.Rows[i]["SessionID"]);
                                    concurrentOpmSessions.Rows.Add(orderedOpmSessions.Rows[i]["Time"], sessionIdDictionary.Count);
                                }
                            }

                            concurrentOpmSessions = (from s in concurrentOpmSessions.AsEnumerable()
                                                     group s by s.Field<DateTime>("Time").ToString("yyyy-MM-dd") into d
                                                     select new
                                                     {
                                                         Date = d.Key,
                                                         Weekday = DateTime.Parse(d.Key).DayOfWeek.ToString(),
                                                         MaxConcurrentSessions = d.Max(row => row.Field<int>("ConcurrentSessions"))
                                                     }).ToDataTable();

                            maximumConcurrentOpmSessions = concurrentOpmSessions.AsEnumerable().Max(row => row.Field<int>("MaxConcurrentSessions"));
                        }

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(DateTime.Now + " An error occurred while trying to determine OPM sessions information");
                        Console.WriteLine(DateTime.Now + " " + ex.Message);
                    }


                    if (DBFunctions.checkIfTableExistsOrIsEmpty("Sessions"))
                    {
                        hasSessions = true;
                        DataTable concurrentSessions = new DataTable();


                        queryString = "Select [PSM Solution], Count(Distinct(Upper([TargetAccount]))) as TargetAccounts from Sessions group by [PSM Solution];";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(targetAccountsByPsmSolution);
                        DBFunctions.closeDBConnection();

                        if (hasRecordings)
                        {
                            queryString = "select [PSM Solution], Count(TargetHosts) as TargetHosts from (Select [PSM Solution], trim(lower([TargetHost])) as TargetHosts from Sessions union select solution, trim(lower(TargetHost)) from sessionRecordings) group by [PSM Solution];";
                        }
                        else
                        {
                            queryString = "Select [PSM Solution], Count(Distinct(Upper(TRIM([TargetHost])))) as TargetHosts from Sessions group by [PSM Solution];";
                        }

                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(targetSystemsByPsmSolution);
                        DBFunctions.closeDBConnection();

                        queryString = "Select [PSM Solution], Count(Distinct(Upper([CyberArk User]))) as [CyberArk User] from Sessions group by [PSM Solution];";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(cyberArkUsersByPsmSolution);
                        DBFunctions.closeDBConnection();

                        queryString = "Select [PSM Solution], Count([ErrorOccurred]) as SessionsWithErrors from Sessions where [ErrorOccurred] = 'Yes' group by [PSM Solution]";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsWithErrors);
                        DBFunctions.closeDBConnection();

                        queryString = "Select [PSM Solution], Count([DurationElapsed]) as SessionDurationElapsed from Sessions where [DurationElapsed] = 'Yes' group by [PSM Solution];";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(elapsedSessions);
                        DBFunctions.closeDBConnection();

                        queryString = "select case When [Ad-Hoc Connection] = 'Yes' then 'Ad-Hoc connections' else 'PSM connections' end as ConnectionType, Sessions from (select [Ad-Hoc Connection], count([Ad-Hoc Connection]) as Sessions from sessions group by [Ad-Hoc Connection] order by Sessions desc);";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByConnectionType);
                        DBFunctions.closeDBConnection();


                        if (hasFiles)
                        {
                            queryString = "select PolicyID, count(PolicyID) as Sessions from sessions where PolicyID is not null group by lower(PolicyID) order by Sessions desc;";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(sessionsByPolicy);
                            DBFunctions.closeDBConnection();

                            queryString = "select DeviceType, count(DeviceType) as Sessions from sessions where DeviceType is not null group by lower(DeviceType) order by Sessions desc;";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(sessionsByDeviceType);
                            DBFunctions.closeDBConnection();
                        }

                        totalNumberOfSessions = sessionsDataTable.Rows.Count;
                        sessionsDataTable.Columns.Add("SessionDuration", typeof(TimeSpan)).SetOrdinal(sessionsDataTable.Columns["EndTime"].Ordinal + 1);

                        for (int i = 0; i < sessionsDataTable.Rows.Count; i++)
                        {
                            sessionsDataTable.Rows[i]["SessionDuration"] = (Convert.ToDateTime(sessionsDataTable.Rows[i]["EndTime"]) - Convert.ToDateTime(sessionsDataTable.Rows[i]["StartTime"]));
                        }

                        averageSessionDuration = TimeSpan.FromSeconds(Convert.ToInt32(sessionsDataTable.AsEnumerable().Average(row => row.Field<TimeSpan>("SessionDuration").TotalSeconds)));

                        sessionsBySessionDuration.Columns.Add("Duration", typeof(string));
                        sessionsBySessionDuration.Columns.Add("Sessions", typeof(int));

                        sessionsBySessionDuration.Rows.Add("Up to one minute", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") <= TimeSpan.FromSeconds(60) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than one minute", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromSeconds(60) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 5 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromMinutes(5) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 15 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromMinutes(15) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 30 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromMinutes(30) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 60 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromMinutes(60) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 3 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromHours(3) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 6 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromHours(6) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 12 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromHours(12) select s).Count());
                        sessionsBySessionDuration.Rows.Add("More than 24 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") > TimeSpan.FromHours(24) select s).Count());

                        sessionsBySessionDuration2.Columns.Add("Duration", typeof(string));
                        sessionsBySessionDuration2.Columns.Add("Sessions", typeof(int));
                        sessionsBySessionDuration2.Columns.Add("Share (%)", typeof(double));
                        sessionsBySessionDuration2.Rows.Add("Less than one minute", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromSeconds(60) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("1 to 5 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromSeconds(60) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromMinutes(5) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("5 to 15 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromMinutes(5) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromMinutes(15) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("15 to 30 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromMinutes(15) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromMinutes(30) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("30 to 60 minutes", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromMinutes(30) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromMinutes(60) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("1 to 3 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromHours(1) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromHours(3) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("3 to 6 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromHours(3) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromHours(6) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("6 to 12 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromHours(6) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromHours(12) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("12 to 24 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromHours(12) && s.Field<TimeSpan>("SessionDuration") < TimeSpan.FromHours(24) select s).Count());
                        sessionsBySessionDuration2.Rows.Add("More than 24 hours", (from s in sessionsDataTable.AsEnumerable() where s.Field<TimeSpan>("SessionDuration") >= TimeSpan.FromHours(24) select s).Count());

                        for (int i = 0; i < sessionsBySessionDuration2.Rows.Count; i++)
                        {
                            sessionsBySessionDuration2.Rows[i][2] = Math.Round((int)sessionsBySessionDuration2.Rows[i][1] * 100.00 / totalNumberOfSessions, 2);
                        }

                        tempDataTable = new DataTable();
                        queryString = "select min(StartTime) from Sessions where endtime is not null;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        sessionsMinimumDate = DateTime.Parse(tempDataTable.Rows[0][0].ToString());
                        DBFunctions.closeDBConnection();

                        tempDataTable = new DataTable();
                        queryString = "select max(EndTime) from Sessions;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        sessionsMaximumDate = DateTime.Parse(tempDataTable.Rows[0][0].ToString());
                        DBFunctions.closeDBConnection();

                        maxConcurrentSessions.Columns.Add("Date", typeof(DateTime));
                        maxConcurrentSessions.Columns.Add("ConcurrentSessions", typeof(int));

                        queryString = "select ConnectionComponent, count(ConnectionComponent) as [Sessions] from Sessions group by lower(ConnectionComponent) order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByConnectionComponent);
                        DBFunctions.closeDBConnection();

                        queryString = "select TargetHost, count(lower(trim(TargetHost))) as [Sessions] from Sessions group by lower(TargetHost) order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByTarget);
                        DBFunctions.closeDBConnection();

                        sessionsByTargetTop10 = sessionsByTarget.AsEnumerable().Take(10).CopyToDataTable();

                        queryString = "select TargetAccount, TargetHost, TargetUser, count(TargetAccount) as [Sessions] from Sessions group by lower(TargetAccount) order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByTargetAccount);
                        DBFunctions.closeDBConnection();

                        sessionsByTargetAccountTop10 = sessionsByTargetAccount.AsEnumerable().Take(10).CopyToDataTable();

                        queryString = "select [Solution Server ID] || ' (' || [PSM Solution] || ')' as SolutionServerID , count([Solution Server ID]) as [Sessions] from Sessions group by lower(SolutionServerID) order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByPsmID);
                        DBFunctions.closeDBConnection();

                        queryString = "select [PSM Solution], count([PSM Solution]) as [Sessions] from Sessions group by [PSM Solution] order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByPsmSolution);
                        DBFunctions.closeDBConnection();

                        queryString = "select [CyberArk User] as User, count([CyberArk User]) as [Sessions] from Sessions group by lower([User]) order by [Sessions] desc;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByCyberArkUser);
                        DBFunctions.closeDBConnection();

                        queryString = "select a.Date, a.Weekday, case when b.Sessions is not null then b.Sessions else 0 End as [Sessions] from (WITH RECURSIVE dates(starttime) AS (VALUES(min(starttime)) UNION ALL SELECT date(starttime, '+1 day') FROM Sessions  WHERE starttime <= max(startTime)) SELECT Date(starttime) as Date, case cast (strftime('%w', starttime) as integer) when 0 then 'Sunday' when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday' when 4 then 'Thursday' when 5 then 'Friday' else 'Saturday' end as Weekday FROM Sessions group by Date) a left join (select Date(starttime) as Date,  count(starttime) as [Sessions] from Sessions group by date(starttime)) b on a.Date = b.Date order by a.date;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(initiatedSessionsPerDay);
                        DBFunctions.closeDBConnection();

                        queryString = "WITH RECURSIVE dates(starttime) AS (VALUES(min(starttime)) UNION ALL SELECT date(starttime, '+1 day') FROM Sessions  WHERE endtime <= max(startTime)) SELECT Date(starttime) as Date, case cast (strftime('%w', starttime) as integer) when 0 then 'Sunday' when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday' when 4 then 'Thursday' when 5 then 'Friday' else 'Saturday' end as Weekday FROM Sessions group by Date order by date;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionDays);
                        DBFunctions.closeDBConnection();

                        queryString = "Select strftime('%H', StartTime) as Hour, count(*) as Sessions from Sessions group by strftime('%H', StartTime) order by Hour;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsByHour);
                        DBFunctions.closeDBConnection();

                        averageSessionDurationByPsmSolution = (from s in sessionsDataTable.AsEnumerable()
                                                               group s by s.Field<string>("PSM Solution") into g
                                                               select new
                                                               {
                                                                   Solution = g.Key,
                                                                   AverageSessionDuration = TimeSpan.FromSeconds(Convert.ToInt32(g.Average(x => x.Field<TimeSpan>("SessionDuration").TotalSeconds)))
                                                               }).ToDataTable();


                        averageSessionDurationByConnectionComponent = (from s in sessionsDataTable.AsEnumerable()
                                                                       group s by s.Field<string>("ConnectionComponent") into g
                                                                       select new
                                                                       {
                                                                           ConnectionComponent = g.Key,
                                                                           AverageSessionDuration = TimeSpan.FromSeconds(Convert.ToInt32(g.Average(x => x.Field<TimeSpan>("SessionDuration").TotalSeconds)))
                                                                       }).OrderByDescending(g => g.AverageSessionDuration).ToDataTable();

                        adHocConnections = (from s in sessionsDataTable.AsEnumerable() where s.Field<string>("Ad-Hoc Connection") == "Yes" select s).Count();

                        tempDataTable = new DataTable();
                        queryString = "select Weekday, sum(sessions) as Sessions from (select a.Day, a.Weekday, case when b.Sessions is not null then b.Sessions else 0 End as [Sessions] from (WITH RECURSIVE dates(starttime) AS (VALUES(min(starttime)) UNION ALL SELECT date(starttime, '+1 day') FROM Sessions  WHERE starttime <= max(startTime)) SELECT Date(starttime) as Date, case cast(strftime('%w', starttime) as integer) when 0 then 7 else cast(strftime('%w', starttime) as integer) End as Day, case cast (strftime('%w', starttime) as integer) when 0 then 'Sunday' when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday' when 4 then 'Thursday' when 5 then 'Friday' else 'Saturday' end as Weekday FROM Sessions group by Date) a left join (select Date(starttime) as Date,  count(starttime) as [Sessions] from Sessions group by date(starttime)) b on a.Date = b.Date order by a.Day) group by Day;";
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(sessionsPerWeekday);
                        DBFunctions.closeDBConnection();


                        tempDataSet = new DataSet();
                        initiatedSessionsByPsmSolutionPerDay = sessionDays.Copy();
                        queryString = "select Date(starttime) as [Date], Count(*) as [Solution] from [Sessions] where [PSM Solution] = $psmSolution group by Date";
                        for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                        {
                            initiatedSessionsByPsmSolutionPerDay.Columns.Add(sessionsByPsmSolution.Rows[i][0].ToString().Trim(), typeof(int));
                            tempDataTable = new DataTable();
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            command.Parameters.AddWithValue("$psmSolution", sessionsByPsmSolution.Rows[i][0].ToString());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(tempDataTable);
                            tempDataSet.Tables.Add(tempDataTable);
                            tempDataSet.Tables[i].TableName = sessionsByPsmSolution.Rows[i][0].ToString().Trim();
                            DBFunctions.closeDBConnection();
                        }
                        foreach (DataTable table in tempDataSet.Tables)
                        {
                            for (int i = 0; i < table.Rows.Count; i++)
                            {
                                for (int j = 0; j < initiatedSessionsByPsmSolutionPerDay.Rows.Count; j++)
                                {
                                    if (table.Rows[i][0].ToString() == initiatedSessionsByPsmSolutionPerDay.Rows[j][0].ToString())
                                    {
                                        initiatedSessionsByPsmSolutionPerDay.Rows[j][table.TableName] = table.Rows[i][1];
                                    }
                                    if (initiatedSessionsByPsmSolutionPerDay.Rows[j][table.TableName].ToString() == "")
                                    {
                                        initiatedSessionsByPsmSolutionPerDay.Rows[j][table.TableName] = 0;
                                    }
                                }
                            }
                        }


                        tempDataSet = new DataSet();
                        initiatedSessionsByPsmIdPerDay = sessionDays.Copy();
                        queryString = "select Date(starttime) as [Date], Count(*) as [PSM] from [Sessions] where [Solution Server ID] = $psmID and [PSM Solution] = $solution group by Date";
                        for (int i = 0; i < sessionsByPsmID.Rows.Count; i++)
                        {
                            initiatedSessionsByPsmIdPerDay.Columns.Add(sessionsByPsmID.Rows[i][0].ToString().Trim(), typeof(int));
                            tempDataTable = new DataTable();
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            command.Parameters.AddWithValue("$psmID", sessionsByPsmID.Rows[i][0].ToString().Split('(')[0].TrimEnd(' '));
                            command.Parameters.AddWithValue("$solution", sessionsByPsmID.Rows[i][0].ToString().Split('(')[1].TrimEnd(')'));

                            da = new SQLiteDataAdapter(command);
                            da.Fill(tempDataTable);
                            tempDataSet.Tables.Add(tempDataTable);
                            tempDataSet.Tables[i].TableName = sessionsByPsmID.Rows[i][0].ToString().Trim();
                            DBFunctions.closeDBConnection();
                        }


                        foreach (DataTable table in tempDataSet.Tables)
                        {
                            for (int i = 0; i < table.Rows.Count; i++)
                            {
                                for (int j = 0; j < initiatedSessionsByPsmIdPerDay.Rows.Count; j++)
                                {
                                    if (table.Rows[i][0].ToString() == initiatedSessionsByPsmIdPerDay.Rows[j][0].ToString())
                                    {
                                        initiatedSessionsByPsmIdPerDay.Rows[j][table.TableName] = table.Rows[i][1];
                                    }
                                    if (initiatedSessionsByPsmIdPerDay.Rows[j][table.TableName].ToString() == "")
                                    {
                                        initiatedSessionsByPsmIdPerDay.Rows[j][table.TableName] = 0;
                                    }
                                }
                            }
                        }


                        tempDataSet = new DataSet();
                        initiatedSessionsByConnectionComponentPerDay = sessionDays.Copy();
                        queryString = "select Date(starttime) as [Date], Count(ConnectionComponent) as [$component] from [Sessions] where [ConnectionComponent] = $component group by Date";
                        for (int i = 0; i < sessionsByConnectionComponent.Rows.Count; i++)
                        {
                            initiatedSessionsByConnectionComponentPerDay.Columns.Add(sessionsByConnectionComponent.Rows[i][0].ToString().Trim(), typeof(int));
                            tempDataTable = new DataTable();
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            command.Parameters.AddWithValue("$component", sessionsByConnectionComponent.Rows[i][0]);
                            da = new SQLiteDataAdapter(command);
                            da.Fill(tempDataTable);
                            tempDataSet.Tables.Add(tempDataTable);
                            tempDataSet.Tables[i].TableName = sessionsByConnectionComponent.Rows[i][0].ToString().Trim();
                            DBFunctions.closeDBConnection();
                        }
                        foreach (DataTable table in tempDataSet.Tables)
                        {
                            for (int i = 0; i < table.Rows.Count; i++)
                            {
                                for (int j = 0; j < initiatedSessionsByConnectionComponentPerDay.Rows.Count; j++)
                                {
                                    if (table.Rows[i][0].ToString() == initiatedSessionsByConnectionComponentPerDay.Rows[j][0].ToString())
                                    {
                                        initiatedSessionsByConnectionComponentPerDay.Rows[j][table.TableName] = table.Rows[i][1];
                                    }
                                    if (initiatedSessionsByConnectionComponentPerDay.Rows[j][table.TableName].ToString() == "")
                                    {
                                        initiatedSessionsByConnectionComponentPerDay.Rows[j][table.TableName] = 0;
                                    }
                                }
                            }
                        }

                        Dictionary<string, int[]> maxConcurrentSessionsByUserDictionary = new Dictionary<string, int[]>();


                        try
                        {

                            DataTable sessionsOrdered = new DataTable();
                            sessionsOrdered.Columns.Add("Code", typeof(int));
                            sessionsOrdered.Columns.Add("Time", typeof(DateTime));
                            sessionsOrdered.Columns.Add("sessionType", typeof(string));
                            sessionsOrdered.Columns.Add("psmSolution", typeof(string));
                            sessionsOrdered.Columns.Add("User", typeof(string));
                            sessionsOrdered.Columns.Add("connectionComponent", typeof(string));
                            sessionsOrdered.Columns.Add("psmID", typeof(string));
                            sessionsOrdered.Columns.Add("SessionID", typeof(string));


                            queryString = "select * from (select 300 as Code, StartTime as Time, [PSM Solution] as psmSolution, [CyberArk User] as User, ConnectionComponent, [Solution Server ID] as psmID, Trim(SessionID) as SessionID from Sessions where Sessions.StartTime <= Sessions.EndTime  union select 302 as Code, EndTime as Time, [PSM Solution], [CyberArk User], ConnectionComponent, [Solution Server ID], Trim(SessionID) from Sessions where Sessions.StartTime <= Sessions.EndTime ) where Time != '' and time is not null order by time";
                            command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                            da = new SQLiteDataAdapter(command);
                            da.Fill(sessionsOrdered);
                            DBFunctions.closeDBConnection();


                            concurrentSessions.Columns.Add("Time", typeof(DateTime));
                            concurrentSessions.Columns.Add("Sessions", typeof(int));

                            maxConcurrentSessionsByPsmSolutionPerDay.Columns.Add("Time", typeof(DateTime));
                            Dictionary<string, int> maxConcurrentSessionsByPsmSolutionDictionary = new Dictionary<string, int>();

                            for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                            {
                                if (!maxConcurrentSessionsByPsmSolutionPerDay.Columns.Contains(sessionsByPsmSolution.Rows[i][0].ToString()))
                                {
                                    maxConcurrentSessionsByPsmSolutionDictionary.Add(sessionsByPsmSolution.Rows[i][0].ToString(), 0);

                                
                                        maxConcurrentSessionsByPsmSolutionPerDay.Columns.Add(sessionsByPsmSolution.Rows[i][0].ToString(), typeof(int));
                                        maxConcurrentSessionsByPsmSolutionPerDay.Columns[sessionsByPsmSolution.Rows[i][0].ToString()].DefaultValue = 0;
                                    
                                }
                            }

                            maxConcurrentSessionsByConnectionComponentPerDay.Columns.Add("Time", typeof(DateTime));
                            maxConcurrentSessionsByConnectionComponent.Columns.Add("ConnectionComponent", typeof(string));
                            maxConcurrentSessionsByConnectionComponent.Columns.Add("ConcurrentSessions", typeof(int));


                            Dictionary<string, int> maxConcurrentSessionsByConnectionComponentPerDayDictionary = new Dictionary<string, int>();
                            for (int i = 0; i < sessionsByConnectionComponent.Rows.Count; i++)
                            {
                                if (!maxConcurrentSessionsByConnectionComponentPerDay.Columns.Contains(sessionsByConnectionComponent.Rows[i][0].ToString()))
                                {
                                    maxConcurrentSessionsByConnectionComponentPerDayDictionary.Add(sessionsByConnectionComponent.Rows[i][0].ToString().ToUpper(), 0);


                                        maxConcurrentSessionsByConnectionComponentPerDay.Columns.Add(sessionsByConnectionComponent.Rows[i][0].ToString().ToUpper(), typeof(int));
                                        maxConcurrentSessionsByConnectionComponentPerDay.Columns[sessionsByConnectionComponent.Rows[i][0].ToString()].DefaultValue = 0;
                                    
                                }
                            }

                            maxConcurrentSessionsByPsmIDPerDay.Columns.Add("Time", typeof(DateTime));
                            maxConcurrentSessionsByPsmID.Columns.Add("PSM Server ID", typeof(string));
                            maxConcurrentSessionsByPsmID.Columns.Add("ConcurrentSessions", typeof(int));

                            Dictionary<string, int> maxConcurrentSessionsByPsmIDDictionary = new Dictionary<string, int>();

                            for (int i = 0; i < sessionsByPsmID.Rows.Count; i++)
                            {
                                string psmID = sessionsByPsmID.Rows[i][0].ToString().Split('(')[0].TrimEnd();

                                if (!maxConcurrentSessionsByPsmIDPerDay.Columns.Contains(psmID) && !sessionsByPsmID.Rows[i][0].ToString().Contains("(PSM for SSH)"))
                                {
                                    maxConcurrentSessionsByPsmIDDictionary.Add(psmID, 0);


                                        maxConcurrentSessionsByPsmIDPerDay.Columns.Add(psmID, typeof(int));
                                        maxConcurrentSessionsByPsmIDPerDay.Columns[psmID].DefaultValue = 0;
                                   
                                   
                                }
                            }

                            maxConcurrentSessionsByUserPerDay.Columns.Add("Time", typeof(DateTime));
                            maxConcurrentSessionsByUser.Columns.Add("Username", typeof(string));
                            maxConcurrentSessionsByUser.Columns.Add("ConcurrentSessions", typeof(int));


                            for (int i = 0; i < sessionsByCyberArkUser.Rows.Count; i++)
                            {
                                string userName = sessionsByCyberArkUser.Rows[i][0].ToString().ToLower();
                                if (!maxConcurrentSessionsByUserPerDay.Columns.Contains(userName))
                                {
                                    maxConcurrentSessionsByUserDictionary.Add(userName, new int[] { 0, 0 });

                                        maxConcurrentSessionsByUserPerDay.Columns.Add(userName, typeof(int));
                                        maxConcurrentSessionsByUserPerDay.Columns[userName].DefaultValue = 0;
                                    
                                }
                            }

                            Dictionary<string, int> sessionsDisctionary = new Dictionary<string, int>();

                            session sessionEntry = new session();
                            string user = string.Empty;

                            for (int i = 0; i < sessionsOrdered.Rows.Count; i++)
                            {

                                string connectionComponent = sessionsOrdered.Rows[i]["connectionComponent"].ToString().ToUpper();
                                string psmID = (string)sessionsOrdered.Rows[i]["psmID"];
                                string psmSolution = (string)sessionsOrdered.Rows[i]["psmSolution"];
                                string userName = sessionsOrdered.Rows[i]["User"].ToString().ToLower(); ;


                                if (((int)sessionsOrdered.Rows[i]["Code"] == 300) && !sessionsDisctionary.ContainsKey(sessionsOrdered.Rows[i]["SessionID"].ToString()))
                                {
                                    sessionsDisctionary.Add((string)sessionsOrdered.Rows[i]["SessionID"], 0);
                                    maxConcurrentSessionsByPsmSolutionDictionary[(string)sessionsOrdered.Rows[i]["psmSolution"]]++;
                                    maxConcurrentSessionsByConnectionComponentPerDayDictionary[connectionComponent]++;
                                    maxConcurrentSessionsByUserDictionary[userName][0]++;

                                    if (maxConcurrentSessionsByUserDictionary[userName][1] < maxConcurrentSessionsByUserDictionary[userName][0])
                                    {
                                        maxConcurrentSessionsByUserDictionary[userName][1] = maxConcurrentSessionsByUserDictionary[userName][0];
                                    }

                                    if (psmSolution != "PSM for SSH")
                                    {
                                        maxConcurrentSessionsByPsmIDDictionary[psmID]++;
                                    }
                                }
                                else if ((int)sessionsOrdered.Rows[i]["Code"] == 302 && sessionsDisctionary.ContainsKey((string)sessionsOrdered.Rows[i]["SessionID"]))
                                {
                                    sessionsDisctionary.Remove((string)sessionsOrdered.Rows[i]["SessionID"]);
                                    maxConcurrentSessionsByPsmSolutionDictionary[(string)sessionsOrdered.Rows[i]["psmSolution"]]--;
                                    maxConcurrentSessionsByConnectionComponentPerDayDictionary[connectionComponent]--;
                                    maxConcurrentSessionsByUserDictionary[userName][0]--;
                                    if (psmSolution != "PSM for SSH")
                                    {
                                        maxConcurrentSessionsByPsmIDDictionary[psmID]--;
                                    }
                                }

                                concurrentSessions.Rows.Add(sessionsOrdered.Rows[i]["Time"], sessionsDisctionary.Count);

                                DataRow solutionDataRow = maxConcurrentSessionsByPsmSolutionPerDay.NewRow();
                                solutionDataRow[0] = sessionsOrdered.Rows[i]["Time"];
                                solutionDataRow[(string)sessionsOrdered.Rows[i]["psmSolution"]] = maxConcurrentSessionsByPsmSolutionDictionary[(string)sessionsOrdered.Rows[i]["psmSolution"]];
                                maxConcurrentSessionsByPsmSolutionPerDay.Rows.Add(solutionDataRow);

                                DataRow connectionComponentDataRow = maxConcurrentSessionsByConnectionComponentPerDay.NewRow();
                                connectionComponentDataRow[0] = sessionsOrdered.Rows[i]["Time"];
                                connectionComponentDataRow[(string)sessionsOrdered.Rows[i]["connectionComponent"]] = maxConcurrentSessionsByConnectionComponentPerDayDictionary[connectionComponent];
                                maxConcurrentSessionsByConnectionComponentPerDay.Rows.Add(connectionComponentDataRow);

                                DataRow userDataRow = maxConcurrentSessionsByUserPerDay.NewRow();
                                userDataRow[0] = sessionsOrdered.Rows[i]["Time"];
                                userDataRow[userName] = maxConcurrentSessionsByUserDictionary[userName][0];
                                maxConcurrentSessionsByUserPerDay.Rows.Add(userDataRow);

                                if (psmSolution != "PSM for SSH")
                                {
                                    DataRow psmIDDataRow = maxConcurrentSessionsByPsmIDPerDay.NewRow();
                                    psmIDDataRow[0] = sessionsOrdered.Rows[i]["Time"];
                                    psmIDDataRow[psmID] = maxConcurrentSessionsByPsmIDDictionary[psmID];
                                    maxConcurrentSessionsByPsmIDPerDay.Rows.Add(psmIDDataRow);
                                }

                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("An error occored while trying to determine concurrent sessions information.");
                            Console.WriteLine("Error: " + ex.Message);
                        }


                        maxConcurrentSessions = (from row in concurrentSessions.AsEnumerable()
                                                 group row by row.Field<DateTime>("Time").Date into grp
                                                 select new
                                                 {
                                                     Date = grp.Key,
                                                     Weekday = grp.Key.DayOfWeek.ToString(),
                                                     MaxConcurrentSessions = grp.Max(row => row.Field<int>("Sessions"))
                                                 }).ToDataTable();


                        maximumConcurrentSessions = maxConcurrentSessions.AsEnumerable().Max(row => row.Field<int>("MaxConcurrentSessions"));


                        if (sessionsByPsmSolution.Rows.Count > 1)
                        {
                            tempDataSet = new DataSet();
                            for (int j = 0; j < sessionsByPsmSolution.Rows.Count; j++)
                            {
                                string psmSolution = sessionsByPsmSolution.Rows[j][0].ToString();
                                tempDataTable = new DataTable();
                                tempDataTable = (from row in maxConcurrentSessionsByPsmSolutionPerDay.AsEnumerable()
                                                 group row by row.Field<DateTime>("Time").Date into grp
                                                 select new
                                                 {
                                                     Date = grp.Key,
                                                     Weekday = grp.Key.DayOfWeek.ToString(),
                                                     MaxSessions = grp.Max(row => row.Field<int>(psmSolution))
                                                 }).ToDataTable();
                                tempDataTable.Columns["MaxSessions"].ColumnName = psmSolution;
                                tempDataTable.PrimaryKey = new DataColumn[] { tempDataTable.Columns["Date"] };
                                tempDataSet.Tables.Add(tempDataTable);
                            }
                            maxConcurrentSessionsByPsmSolutionPerDay = new DataTable();
                            foreach (DataTable dataTable in tempDataSet.Tables)
                            {
                                maxConcurrentSessionsByPsmSolutionPerDay.Merge(dataTable);
                            }
                        }
                        else if (sessionsByPsmSolution.Rows.Count == 1)
                        {
                            maxConcurrentSessionsByPsmSolutionPerDay = maxConcurrentSessions.Copy();
                            maxConcurrentSessionsByPsmSolutionPerDay.Columns[2].ColumnName = sessionsByPsmSolution.Rows[0][0].ToString();
                        }



                        if (sessionsByConnectionComponent.Rows.Count > 1)
                        {
                            tempDataSet = new DataSet();
                            for (int j = 0; j < sessionsByConnectionComponent.Rows.Count; j++)
                            {
                                string connectionComponent = sessionsByConnectionComponent.Rows[j][0].ToString();
                                tempDataTable = new DataTable();
                                tempDataTable = (from row in maxConcurrentSessionsByConnectionComponentPerDay.AsEnumerable()
                                                 group row by row.Field<DateTime>("Time").Date into grp
                                                 select new
                                                 {
                                                     Date = grp.Key,
                                                     Weekday = grp.Key.DayOfWeek.ToString(),
                                                     MaxSessions = grp.Max(row => row.Field<int>(connectionComponent))
                                                 }).ToDataTable();

                                maxConcurrentSessionsByConnectionComponent.Rows.Add(connectionComponent, tempDataTable.AsEnumerable().Max(x => x.Field<int>("MaxSessions")));

                                tempDataTable.Columns["MaxSessions"].ColumnName = connectionComponent;
                                tempDataTable.PrimaryKey = new DataColumn[] { tempDataTable.Columns["Date"] };
                                tempDataSet.Tables.Add(tempDataTable);

                            }

                            maxConcurrentSessionsByConnectionComponent.DefaultView.Sort = "ConcurrentSessions Desc";
                            maxConcurrentSessionsByConnectionComponent = maxConcurrentSessionsByConnectionComponent.DefaultView.ToTable();

                            maxConcurrentSessionsByConnectionComponentPerDay = new DataTable();
                            foreach (DataTable dataTable in tempDataSet.Tables)
                            {
                                maxConcurrentSessionsByConnectionComponentPerDay.Merge(dataTable);
                            }
                        }
                        else if (sessionsByConnectionComponent.Rows.Count == 1)
                        {
                            maxConcurrentSessionsByConnectionComponentPerDay = maxConcurrentSessions.Copy();
                            maxConcurrentSessionsByConnectionComponentPerDay.Columns[2].ColumnName = sessionsByConnectionComponent.Rows[0][0].ToString();
                        }


                        if (sessionsByCyberArkUser.Rows.Count > 0)
                        {
                            tempDataSet = new DataSet();
                            maxConcurrentSessionsByUserDictionary = (from entry in maxConcurrentSessionsByUserDictionary orderby entry.Value[1] descending select entry).ToDictionary();
                            int counter = 0;

                            foreach (KeyValuePair<string, int[]> entry in maxConcurrentSessionsByUserDictionary)
                            {
                                maxConcurrentSessionsByUser.Rows.Add(entry.Key, entry.Value[1]);

                                if (counter < 100)
                                {
                                    string user = entry.Key.ToLower();
                                    tempDataTable = new DataTable();
                                    tempDataTable = (from row in maxConcurrentSessionsByUserPerDay.AsEnumerable()
                                                     group row by row.Field<DateTime>("Time").Date into grp
                                                     select new
                                                     {
                                                         Date = grp.Key,
                                                         Weekday = grp.Key.DayOfWeek.ToString(),
                                                         MaxSessions = grp.Max(row => row.Field<int>(user))
                                                     }).ToDataTable();
                                    tempDataTable.Columns["MaxSessions"].ColumnName = user;
                                    tempDataTable.PrimaryKey = new DataColumn[] { tempDataTable.Columns["Date"] };
                                    tempDataSet.Tables.Add(tempDataTable);
                                }
                                counter++;
                            }

                            maxConcurrentSessionsByUserPerDay = new DataTable();
                            foreach (DataTable dataTable in tempDataSet.Tables)
                            {
                                maxConcurrentSessionsByUserPerDay.Merge(dataTable);
                            }
                        }


                        if (sessionsByPsmID.Rows.Count > 0)
                        {


                            tempDataSet = new DataSet();
                            for (int j = 0; j < sessionsByPsmID.Rows.Count; j++)
                            {
                                if (!sessionsByPsmID.Rows[j][0].ToString().Contains("PSM for SSH"))
                                {
                                    string psmID = sessionsByPsmID.Rows[j][0].ToString().Split('(')[0].TrimEnd();
                                    tempDataTable = new DataTable();
                                    tempDataTable = (from row in maxConcurrentSessionsByPsmIDPerDay.AsEnumerable()
                                                     group row by row.Field<DateTime>("Time").Date into grp
                                                     select new
                                                     {
                                                         Date = grp.Key,
                                                         Weekday = grp.Key.DayOfWeek.ToString(),
                                                         MaxSessions = grp.Max(row => row.Field<int>(psmID))
                                                     }).ToDataTable();

                                    maxConcurrentSessionsByPsmID.Rows.Add(psmID + " (PSM)", tempDataTable.AsEnumerable().Max(x => x.Field<int>("MaxSessions")));

                                    tempDataTable.Columns["MaxSessions"].ColumnName = psmID + " (PSM)";
                                    tempDataTable.PrimaryKey = new DataColumn[] { tempDataTable.Columns["Date"] };
                                    tempDataSet.Tables.Add(tempDataTable);
                                }
                            }

                            maxConcurrentSessionsByPsmID.DefaultView.Sort = "ConcurrentSessions Desc";
                            maxConcurrentSessionsByPsmID = maxConcurrentSessionsByPsmID.DefaultView.ToTable();

                            maxConcurrentSessionsByPsmIDPerDay = new DataTable();
                            foreach (DataTable dataTable in tempDataSet.Tables)
                            {
                                maxConcurrentSessionsByPsmIDPerDay.Merge(dataTable);
                            }
                        }


                        if (hasRecordings)
                        {
                            queryString = "select count(*) from (select lower(trim(targetHost)) as TargetHost from sessions union select lower(trim(targetHost)) from sessionRecordings)";
                        }
                        else
                        {
                            queryString = "select Count (distinct TargetHosts) from (select lower(trim(targetHost)) as TargetHosts from sessions)";
                        }


                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        sessionTargetSystems = Convert.ToInt32(command.ExecuteScalar());
                        DBFunctions.closeDBConnection();

                        sessionTargetAccounts = (from x in sessionsDataTable.AsEnumerable() select x.Field<string>("TargetAccount")).Distinct(StringComparer.CurrentCultureIgnoreCase).Count();
                        sessionUsers = (from x in sessionsDataTable.AsEnumerable() select x.Field<string>("CyberArk User")).Distinct(StringComparer.CurrentCultureIgnoreCase).Count();

                        sessionStatisticsDataTable.Rows.Add("Total number of sessions", string.Format("{0:#,##0}", totalNumberOfSessions), "Within the last " + totalLogsDays + " days");
                        if (sessionsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(sessionsByPsmSolution.Rows[i][0] + " sessions", string.Format("{0:#,##0}", sessionsByPsmSolution.Rows[i][1]), Math.Round(Convert.ToDouble(sessionsByPsmSolution.Rows[i][1]) * 100.00 / totalNumberOfSessions, 2).ToString() + "% of all sessions");
                            }
                        }
                        sessionStatisticsDataTable.Rows.Add("Average sessions per day", string.Format("{0:#,##0}", totalNumberOfSessions / totalLogsDays), "Rounded to whole number");
                        if (sessionsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(sessionsByPsmSolution.Rows[i][0] + " average sessions per day", string.Format("{0:#,##0}", Convert.ToInt32(sessionsByPsmSolution.Rows[i][1]) / totalLogsDays), "Rounded to whole number");
                            }
                        }

                        if (adHocConnections > 0)
                        {
                            sessionStatisticsDataTable.Rows.Add("Ad-Hoc connections", string.Format("{0:#,##0}", adHocConnections), Math.Round(adHocConnections * 100.00 / totalNumberOfSessions, 2).ToString() + "% of all sessions");
                        }


                        sessionStatisticsDataTable.Rows.Add("Maximum sessions per day", string.Format("{0:#,##0}", initiatedSessionsPerDay.AsEnumerable().Max(row => row.Field<Int64>("Sessions"))));

                        if (sessionsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(sessionsByPsmSolution.Rows[i][0] + " maximum sessions per day", string.Format("{0:#,##0}", initiatedSessionsByPsmSolutionPerDay.AsEnumerable().Max(row => row.Field<int>(sessionsByPsmSolution.Rows[i][0].ToString()))));
                            }
                        }

                        sessionStatisticsDataTable.Rows.Add("Average session duration", string.Format("{0:#,##0}", averageSessionDuration.ToString()));

                        if (averageSessionDurationByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < averageSessionDurationByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(averageSessionDurationByPsmSolution.Rows[i][0] + " average session duration", averageSessionDurationByPsmSolution.Rows[i][1]);
                            }
                        }

                        sessionStatisticsDataTable.Rows.Add("Maximum concurrent sessions", string.Format("{0:#,##0}", maximumConcurrentSessions));
                        if (sessionsByPsmSolution.Rows.Count > 1 && maxConcurrentSessionsByPsmSolutionPerDay.Rows.Count > 1)
                        {
                            for (int i = 0; i < sessionsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(sessionsByPsmSolution.Rows[i][0] + " maximum concurrent sessions", string.Format("{0:#,##0}", maxConcurrentSessionsByPsmSolutionPerDay.AsEnumerable().Max(row => row.Field<int>(sessionsByPsmSolution.Rows[i][0].ToString()))));
                            }
                        }
                        if (maximumConcurrentOpmSessions > 0)
                        {
                            sessionStatisticsDataTable.Rows.Add("Maximum concurrent OPM sessions", string.Format("{0:#,##0}", maximumConcurrentOpmSessions));

                        }


                        if (hasRecordings)
                        {
                            sessionStatisticsDataTable.Rows.Add("Total distinct target systems", string.Format("{0:#,##0}", sessionTargetSystems), "Based on session audit and session recordings information");
                        }
                        else
                        {
                            sessionStatisticsDataTable.Rows.Add("Total distinct target systems", string.Format("{0:#,##0}", sessionTargetSystems));
                        }



                        if (targetSystemsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < targetSystemsByPsmSolution.Rows.Count; i++)
                            {
                                if (hasRecordings)
                                {
                                    sessionStatisticsDataTable.Rows.Add(targetSystemsByPsmSolution.Rows[i][0] + " distinct target systems", string.Format("{0:#,##0}", targetSystemsByPsmSolution.Rows[i][1]), "Based on session audit and session recordings information");

                                }
                                else
                                {
                                    sessionStatisticsDataTable.Rows.Add(targetSystemsByPsmSolution.Rows[i][0] + " distinct target systems", string.Format("{0:#,##0}", targetSystemsByPsmSolution.Rows[i][1]));
                                }
                            }
                        }


                        sessionStatisticsDataTable.Rows.Add("Total distinct target accounts", string.Format("{0:#,##0}", sessionTargetAccounts));
                        if (targetAccountsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < targetAccountsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(targetAccountsByPsmSolution.Rows[i][0] + " distinct target accounts", string.Format("{0:#,##0}", targetAccountsByPsmSolution.Rows[i][1]));
                            }
                        }

                        sessionStatisticsDataTable.Rows.Add("Total distinct CyberArk users", string.Format("{0:#,##0}", sessionUsers));

                        if (cyberArkUsersByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < cyberArkUsersByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add(cyberArkUsersByPsmSolution.Rows[i][0] + " distinct CyberArk users", string.Format("{0:#,##0}", cyberArkUsersByPsmSolution.Rows[i][1]));
                            }
                        }


                        queryString = "select * from v_failedPsmSessions union select * from v_failedAdhocSessions;";
                        tempDataTable = new DataTable();
                        command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                        da = new SQLiteDataAdapter(command);
                        da.Fill(tempDataTable);
                        DBFunctions.closeDBConnection();


                        if (tempDataTable.Rows.Count > 0)
                        {
                            for (int i = 0; i < tempDataTable.Rows.Count; i++)
                            {
                                sessionsDataTable.Rows.Add(tempDataTable.Rows[i].ItemArray);
                            }
                            sessionsDataTable = sessionsDataTable.AsEnumerable().OrderBy(x => x.Field<string>("StartTime")).CopyToDataTable();
                        }
                    }
                }



                if (allRecordings > 0)
                {
                    if (usersDataTable.AsEnumerable().Where(x => x.Field<string>("UserType").Contains("PSM")).ToList().Count == 0)
                    {
                        Console.WriteLine(Environment.NewLine + "Warning: No PSM component users (e. g. PSM or PSM for SSH users) could be determined from the EVD userlist export. This can lead to issues or incomplete sessions reports information in this tool. Please make sure to add a userlist EVD export to the tool that contains the component users as well.");
                    }

                    sessionStatisticsDataTable.Rows.Add("Recorded sessions", string.Format("{0:#,##0}", allRecordings));
                    if (recordingsByPsmSolution.Rows.Count > 1 && !recordingsByPsmSolution.AsEnumerable().Any(row => row.Field<String>("PSM Solution") == "Unknown"))
                    {
                        for (int i = 0; i < recordingsByPsmSolution.Rows.Count; i++)
                        {
                            sessionStatisticsDataTable.Rows.Add("Recorded " + recordingsByPsmSolution.Rows[i][0] + " sessions", string.Format("{0:#,##0}", recordingsByPsmSolution.Rows[i][1]));
                        }
                    }

   
                        averageRecordingDuration = TimeSpan.FromSeconds(Convert.ToInt32(sessionRecordings.AsEnumerable().Average(row => row.Field<TimeSpan>("RecordingDuration").TotalSeconds)));
                        averageRecordingDurationBySolution = (from s in sessionRecordings.AsEnumerable()
                                                              group s by s.Field<string>("Solution") into g
                                                              select new
                                                              {
                                                                  Solution = g.Key,
                                                                  AverageSessionDuration = TimeSpan.FromSeconds(Convert.ToInt32(g.Average(x => x.Field<TimeSpan>("RecordingDuration").TotalSeconds)))
                                                              }).ToDataTable();

                        sessionStatisticsDataTable.Rows.Add("Average recording duration", averageRecordingDuration);
                        if (sessionStatisticsDataTable.Rows.Count > 1)
                        {
                            for (int i = 0; i < averageRecordingDurationBySolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add("Average " + averageRecordingDurationBySolution.Rows[i][0] + " recording duration", averageRecordingDurationBySolution.Rows[i][1]);
                            }
                        }

                        sessionStatisticsDataTable.Rows.Add("Average recording size / minute", Math.Round((allRecordingsSize * 1024.00 / allRecordings) / averageRecordingDuration.TotalMinutes, 4) + " MB");
                        if (recordingsByPsmSolution.Rows.Count > 1)
                        {
                            for (int i = 0; i < recordingsByPsmSolution.Rows.Count; i++)
                            {
                                sessionStatisticsDataTable.Rows.Add("Average " + recordingsByPsmSolution.Rows[i][0] + " recording size / minute", string.Format("{0:#,##0}", Math.Round(Convert.ToDouble(recordingsByPsmSolution.Rows[i]["Size"]) * 1024.00 / Convert.ToInt32(recordingsByPsmSolution.Rows[i]["Recordings"]) / ((TimeSpan)averageRecordingDurationBySolution.Rows[i][1]).TotalMinutes, 4) + " MB"));
                            }
                        }


                    if (allRecordingFiles > 0)
                    {
                        sessionStatisticsDataTable.Rows.Add("Recording files", string.Format("{0:#,##0}", allRecordingFiles));
                    }
                    if (recordingFilesBySolution.Rows.Count > 1)
                    {
                        for (int i = 0; i < recordingFilesBySolution.Rows.Count; i++)
                        {
                            sessionStatisticsDataTable.Rows.Add(recordingFilesBySolution.Rows[i][0] + " recording files", string.Format("{0:#,##0}", recordingFilesBySolution.Rows[i][1]));
                        }
                    }

                    sessionStatisticsDataTable.Rows.Add("Size of all recordings", string.Format("{0:#,##0}", Math.Round(allRecordingsSize, 2) + " GB"));
                    if (recordingsByPsmSolution.Rows.Count > 1 && !recordingsByPsmSolution.AsEnumerable().Any(row => row.Field<String>("PSM Solution") == "Unknown"))
                    {
                        for (int i = 0; i < recordingsByPsmSolution.Rows.Count; i++)
                        {
                            sessionStatisticsDataTable.Rows.Add(recordingsByPsmSolution.Rows[i][0] + " recordings size", string.Format("{0:#,##0}", Math.Round((double)recordingsByPsmSolution.Rows[i][2], 2) + " GB"));
                        }
                    }
                    sessionStatisticsDataTable.Rows.Add("Average recording size", string.Format("{0:#,##0}", Math.Round(allRecordingsSize * 1024.00 / allRecordings, 3) + " MB"));
                    if (recordingsByPsmSolution.Rows.Count > 1 && !recordingsByPsmSolution.AsEnumerable().Any(row => row.Field<String>("PSM Solution") == "Unknown"))
                    {
                        for (int i = 0; i < recordingsByPsmSolution.Rows.Count; i++)
                        {
                            sessionStatisticsDataTable.Rows.Add("Average " + recordingsByPsmSolution.Rows[i][0] + " recording size", string.Format("{0:#,##0}", Math.Round(Convert.ToDouble(recordingsByPsmSolution.Rows[i]["Size"]) * 1024.00 / Convert.ToInt32(recordingsByPsmSolution.Rows[i]["Recordings"]), 3) + " MB"));
                        }
                    }

                    sessionStatisticsDataTable.Rows.Add("Safes with recordings", string.Format("{0:#,##0}", recordingsPerSafe.Rows.Count));

                }

                if (!skipDeterminingOPMPolicyRules && !MainWindow.skipRestApiActions && settings.getPolicyAclInformation)
                {
                    opmPolicyRules = await determineOpmPolicyRules();
                }

                if (hasPoliciesXML)
                {
                    string policiesXmlFile = MainWindow.configurationFilesList.Where(x => x.Type == "Policies XML file").FirstOrDefault().Linktofile;
                    opmCommandGroups = await determineOpmCommandGroups(policiesXmlFile);
                    connectionComponentsUsage = await determineAssignedConnectionComponents(policiesXmlFile);
                }

                if (hasLicenseXML && hasUsers)
                {
                    licenceCapacity = processLicenseXMLFile(MainWindow.configurationFilesList.Where(x => x.Type == "License XML file").FirstOrDefault().Linktofile);
                    DBFunctions.storeDataTableInSqliteDatabase(licenceCapacity, "LicenseCapacity");
                }
                else if (hasLicenseXML && !hasUsers)
                {
                    Console.WriteLine(Environment.NewLine + "Information: Please note, that the license capacity report will only be created if the License XML file is processed along with the \"Users\" EVD export and its dependend EVD exports. The license capacity report will not be created." + Environment.NewLine);
                }

                if (hasSessions && DBFunctions.checkIfTableExistsOrIsEmpty("tempUsers"))
                {
                    int averageUserLogRetention = 0;
                    queryString = "select avg(LogRetentionPeriod) from tempUsers where UserType = 'EPV'";
                    SQLiteCommand command = new SQLiteCommand(queryString, DBFunctions.connectToDB());
                    averageUserLogRetention = Convert.ToInt32(command.ExecuteScalar());
                    DBFunctions.closeDBConnection();

                    if (averageUserLogRetention < totalLogsDays)
                    {
                        Console.WriteLine(Environment.NewLine + "Warning: Audit logs from " + totalLogsDays + " days were analyzed. However, the average audit log retention setting of EPV users which also contains session audit information is " + averageUserLogRetention + " days. As a result, session audit information can be partially or entirely missing for many days which negatively impacts the accuracy of the session reports in the session management section. For instance, according charts and KPIs might not reflect the actual number of sessions that have occurred.");
                    }
                }


                if (missingPolicies.Count() > 0 && MainWindow.platformPoliciesList.Count > 0)
                {
                    Console.WriteLine("Info: The account management and account compliance could not be determined for " + missingPolicies.Count() + " policies. All relevant policy files can be found in the policies folder of the PasswordManagerShared safe. In ADDITION the REST API integration of this tool should be used that determines policies information as well. It appears that the required policy information for the following policy IDs is missing:");
                    foreach (string policy in missingPolicies)
                    {
                        Console.WriteLine("-" + policy);
                    }
                }
        }





        public static void processVaultTraceFiles()
        {
            processingTraces = true;

            try
            {

                List<MainWindow.ConfigurationFile> traceFilesList = MainWindow.configurationFilesList.Where(x => x.Type == "Vault trace file").OrderBy(x => x.Filename).ToList();

                int numberOfTraces = traceFilesList.Count;


                if (numberOfTraces > 0)
                {
                    totalExecutionTime = 0;
                    coveredTransactionMinutes = 0;
                    transactionUsers = 0;
                    List<string> specificUsersList = new List<string>();
                    specificUsersList.Add("batch");
                    specificUsersList.Add("builtin");


                    String line = string.Empty;
                    String result = string.Empty;
                    Stopwatch sw = new Stopwatch();
                    DateTime timeStamp = new DateTime();
                    string[] parts = new string[] { };
                    string[] outputs = new string[] { };
                    Int64 unixTimeMilliseconds = 0;
                    Int32 ID = 0;
                    Int64 outputSize = 0;
                    Int64 outputRecords = 0;
                    int index = 0;
                    int subID = 0;
                    string key = String.Empty;
                    string subIdKey = String.Empty;
                    string data = string.Empty;
                    string success = string.Empty;
                    string IP = string.Empty;
                    string userName = string.Empty;
                    string masterID = string.Empty;

                    Dictionary<string, Dictionary<string, int>> subIdDictionary = new Dictionary<string, Dictionary<string, int>>();
                    Dictionary<string, int> value;
                    Dictionary<string, traceEntry> traceDictionary = new Dictionary<string, traceEntry>();
                    Dictionary<string, string> userTypeDictionary = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                    if (hasNonEvdUserList)
                    {
                        userTypeDictionary = processUserList(MainWindow.configurationFilesList.Where(x => x.Type == "User list (non-EVD)").FirstOrDefault().Linktofile);
                    }
                    else if (usersDataTable != null && usersDataTable.Rows.Count > 0 && usersDataTable.Columns.Contains("UserName") && usersDataTable.Columns.Contains("UserType"))
                    {
                        DataTable userTypesDataTable = new DataTable();
                        DataView view = new DataView(usersDataTable);
                        userTypesDataTable = view.ToTable(true, "UserName", "UserType");
                        userTypeDictionary = userTypesDataTable.AsEnumerable().ToDictionary<DataRow, string, string>(row => row.Field<string>(0), row => row.Field<string>(1), StringComparer.OrdinalIgnoreCase);
                    }


                    concurrentVaultTasks = new DataTable();
                    concurrentVaultTasks.Columns.Add("Time", typeof(DateTime));
                    concurrentVaultTasks.Columns.Add("TotalTransactions", typeof(int));
                    concurrentVaultTasks.Columns.Add("AAMProvider", typeof(int));
                    concurrentVaultTasks.Columns.Add("CPM", typeof(int));
                    concurrentVaultTasks.Columns.Add("DR", typeof(int));
                    concurrentVaultTasks.Columns.Add("Backup", typeof(int));
                    concurrentVaultTasks.Columns.Add("PVWAAPP", typeof(int));
                    concurrentVaultTasks.Columns.Add("PVWAGW", typeof(int));
                    concurrentVaultTasks.Columns.Add("PSMAPP", typeof(int));
                    concurrentVaultTasks.Columns.Add("PSMGW", typeof(int));
                    concurrentVaultTasks.Columns.Add("PSMforSSHAPP", typeof(int));
                    concurrentVaultTasks.Columns.Add("PSMforSSHGW", typeof(int));
                    concurrentVaultTasks.Columns.Add("PSMforSSHADB", typeof(int));
                    concurrentVaultTasks.Columns.Add("Builtin", typeof(int));
                    concurrentVaultTasks.Columns.Add("OPMAgent", typeof(int));
                    concurrentVaultTasks.Columns.Add("PTA", typeof(int));
                    concurrentVaultTasks.Columns.Add("EPV", typeof(int));
                    concurrentVaultTasks.Columns.Add("Other", typeof(int));


                    DateTime fileStartTime = new DateTime();
                    DateTime fileEndTime = DateTime.MinValue;
                    Task writeToDatabaseTask = null;

                    Hashtable hashtable = new Hashtable();
                    int keyCounter = 0;
                    int traceFileCounter = 0;
                    int fileCounter = 0;
                    bool timeGapsFound = false;
                    string dateTimeString = string.Empty;
                    string serviceName = string.Empty;
                    fileEndTime = DateTime.MinValue;

                    Dictionary<string, long[]> concurrentTransactionsPerUser = new Dictionary<string, long[]>();
                    Dictionary<string, Dictionary<string, int[]>> concurrentTransactionsPerUserByTime = new Dictionary<string, Dictionary<string, int[]>>();
                    concurrentTransactionsPerService = new Dictionary<string, long[]>();
                    Dictionary<string, Dictionary<string, int[]>> concurrentTransactionsPerServiceByTime = new Dictionary<string, Dictionary<string, int[]>>();

                    maxConcurrentTransactionsByUser = new DataTable();
                    maxConcurrentTransactionsByUser.Columns.Add("Username", typeof(string));
                    maxConcurrentTransactionsByUser.Columns.Add("UserType", typeof(string));
                    maxConcurrentTransactionsByUser.Columns.Add("ConcurrentTransactions", typeof(int));


                    maxTransactionsByUserTime = new DataTable();
                    maxTransactionsByUserTime.Columns.Add("Time", typeof(string));

                    maxConcurrentTransactionsByService = new DataTable();
                    maxConcurrentTransactionsByService.Columns.Add("Service", typeof(string));
                    maxConcurrentTransactionsByService.Columns.Add("ConcurrentTransactions", typeof(int));

                    maxTransactionsByServiceTime = new DataTable();
                    maxTransactionsByServiceTime.Columns.Add("Time", typeof(string));

                    Dictionary<string, long> averageConcurrentTransactionsByUserTypeDictionary = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("Total", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("AAM Provider", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("CPM", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("DR", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("Backup", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PVWA App", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PVWA GW", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PSM APP", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PSM GW", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PSM for SSH APP", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PSM for SSH GW", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PSM for SSH ADB", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("Builtin", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("OPM Agent", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("PTA", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("EPV", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("Other", 0);
                    averageConcurrentTransactionsByUserTypeDictionary.Add("Counter", 0);


                    HashSet<string> dateTimeHashset = new HashSet<string>();

                    sw.Start();

                    foreach (MainWindow.ConfigurationFile traceFile in traceFilesList)
                    {
                        fileCounter++;
                        string traceFileNumberString = string.Empty;

                        if (numberOfTraces < 10)
                        {
                            traceFileNumberString = fileCounter.ToString("D1");
                        }
                        else if (traceFileCounter < 100)
                        {
                            traceFileNumberString = fileCounter.ToString("D2");
                        }
                        else if (traceFileCounter < 1000)
                        {
                            traceFileNumberString = fileCounter.ToString("D3");
                        }
                        else if (traceFileCounter < 10000)
                        {
                            traceFileNumberString = fileCounter.ToString("D4");
                        }
                        else
                        {
                            traceFileNumberString = fileCounter.ToString();
                        }

                        Console.WriteLine(DateTime.Now + " Processing file " + traceFileNumberString + "/" + numberOfTraces + " | " + Path.GetFileName(traceFile.Linktofile) + "...");

                        fileStartTime = DateTime.MinValue;

                        using (StreamReader reader = new StreamReader(traceFile.Linktofile))
                        {
                            while ((line = reader.ReadLine()) != null)
                            {
                                line = line.Replace("\"", "").Replace("'", "");
                                parts = line.TrimStart().Split(new char[2] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                                subIdKey = "";
                                data = "";

                                if (fileStartTime == DateTime.MinValue)
                                {
                                    if (fileEndTime != DateTime.MinValue && DateTime.TryParseExact(parts[0] + " " + parts[1], "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out fileStartTime))
                                    {
                                        if (fileStartTime.Subtract(fileEndTime).TotalMinutes > 5)
                                        {
                                            timeGapsFound = true;
                                            TimeSpan missingTimeSpan = fileStartTime.Subtract(fileEndTime);
                                            Console.WriteLine(Environment.NewLine + DateTime.Now + " Warning: There appears to be no Vault transcation information within a timespan of " + ToReadableString(missingTimeSpan) + " which can indicate that trace files that cover this timespan have not been added to this tool." + Environment.NewLine);
                                            hashtable = new Hashtable();
                                        }
                                    }
                                }

                                if (line.Contains("]"))
                                {
                                    serviceName = Regex.Replace(line.Split(']')[1].Split((char)0x1f)[0].Replace("ServiceName=", "").Replace("0=", "").Split('�')[0].Trim(), "[^0-9a-zA-Z]+", "");
                                }

                                if (parts.Length > 5 && parts[4] == "PE")
                                {

                                    string user = parts[6].Split('[')[0].ToLower().Trim();

                                    if (!userTypeDictionary.ContainsKey(user))
                                    {

                                        string type = string.Empty;

                                        if (Regex.IsMatch(user, settings.ProviderRegex))
                                        {
                                            type = "AAM Provider";
                                        }
                                        else if (Regex.IsMatch(user, settings.CPMRegex))
                                        {
                                            type = "CPM";
                                        }
                                        else if (Regex.IsMatch(user, settings.BuiltinRegex))
                                        {
                                            type = "Builtin";
                                        }
                                        else if (Regex.IsMatch(user, settings.BackupRegex))
                                        {
                                            type = "Backup";
                                        }
                                        else if (Regex.IsMatch(user, settings.EPVRegex))
                                        {
                                            type = "EPV";
                                        }
                                        else if (Regex.IsMatch(user, settings.DRRegex))
                                        {
                                            type = "DR";
                                        }
                                        else if (Regex.IsMatch(user, settings.PVWAAppRegex))
                                        {
                                            type = "PVWA APP";
                                        }
                                        else if (Regex.IsMatch(user, settings.PVWAGWRegex))
                                        {
                                            type = "PVWA GW";
                                        }
                                        else if (Regex.IsMatch(user, settings.PSMAppRegex))
                                        {
                                            type = "PSM APP";
                                        }
                                        else if (Regex.IsMatch(user, settings.PSMGWRegex))
                                        {
                                            type = "PSM GW";
                                        }
                                        else if (Regex.IsMatch(user, settings.PSMPAppRegex))
                                        {
                                            type = "PSM for SSH APP";
                                        }
                                        else if (Regex.IsMatch(user, settings.PSMPGWRegex))
                                        {
                                            type = "PSM for SSH GW";
                                        }
                                        else if (Regex.IsMatch(user, settings.PSMPADBRegex))
                                        {
                                            type = "PSM for SSH ADB";
                                        }
                                        else if (Regex.IsMatch(user, settings.PTAAppRegex))
                                        {
                                            type = "PTA";
                                        }
                                        else if (Regex.IsMatch(user, settings.OPMAgentRegex))
                                        {
                                            type = "OPM Agent";
                                        }
                                        else
                                        {
                                            type = settings.AllOtherUsers;
                                        }
                                        userTypeDictionary.Add(user, type);
                                    }

                                    if (!concurrentTransactionsPerUser.ContainsKey(user))
                                    {
                                        concurrentTransactionsPerUser.Add(user, new long[] { 0, 0, 0 });
                                    }
                                    else
                                    {
                                        concurrentTransactionsPerUser[user][0] = 0;
                                    }

                                    if (!concurrentTransactionsPerUserByTime.ContainsKey(user))
                                    {
                                        concurrentTransactionsPerUserByTime.Add(user, new Dictionary<string, int[]>());
                                    }

                                    if (!concurrentTransactionsPerService.ContainsKey(serviceName))
                                    {
                                        concurrentTransactionsPerService.Add(serviceName, new long[] { 0, 0, 0 });
                                    }
                                    else
                                    {
                                        concurrentTransactionsPerService[serviceName][0] = 0;
                                    }

                                    if (!concurrentTransactionsPerServiceByTime.ContainsKey(serviceName))
                                    {
                                        concurrentTransactionsPerServiceByTime.Add(serviceName, new Dictionary<string, int[]>());
                                    }


                                    if (parts[3] == "START")
                                    {
                                        hashtable[(object)parts[2]] = (object)new PEUsage(true, user, parts[5], serviceName);
                                    }
                                    else
                                    {
                                        hashtable[(object)parts[2]] = (object)new PEUsage(false, user, parts[5], serviceName);
                                    }


                                    DateTime dateTime = ToDateTime(parts[0] + " " + parts[1]);
                                    dateTimeString = dateTime.ToString("yyyy-MM-dd HH:mm");

                                    if (!dateTimeHashset.Contains(dateTimeString))
                                    {
                                        dateTimeHashset.Add(dateTimeString);
                                        maxTransactionsByUserTime.Rows.Add(dateTimeString);
                                        maxTransactionsByServiceTime.Rows.Add(dateTimeString);
                                    }

                                    if (concurrentTransactionsPerUserByTime[user].ContainsKey(dateTimeString))
                                    {
                                        concurrentTransactionsPerUserByTime[user][dateTimeString][0] = 0;
                                    }
                                    else
                                    {
                                        concurrentTransactionsPerUserByTime[user].Add(dateTimeString, new int[2] { 0, 0 });
                                    }

                                    if (concurrentTransactionsPerServiceByTime[serviceName].ContainsKey(dateTimeString))
                                    {
                                        concurrentTransactionsPerServiceByTime[serviceName][dateTimeString][0] = 0;
                                    }
                                    else
                                    {
                                        concurrentTransactionsPerServiceByTime[serviceName].Add(dateTimeString, new int[2] { 0, 0 });
                                    }

                                    int aamTasks = 0;
                                    int cpmTasks = 0;
                                    int drTasks = 0;
                                    int pvwaApp = 0;
                                    int pvwaGW = 0;
                                    int psmApp = 0;
                                    int psmGW = 0;
                                    int psmForSshApp = 0;
                                    int psmForSshGW = 0;
                                    int psmForSshADB = 0;
                                    int totalTasks = 0;
                                    int backup = 0;
                                    int builtin = 0;
                                    int opmAgent = 0;
                                    int pta = 0;
                                    int epv = 0;
                                    int other = 0;

                                    var peInUse = hashtable.Values.Cast<PEUsage>().Where(pe => pe.InUse == true);
                                    totalTasks = peInUse.Count();

                                    foreach (PEUsage peUsage in peInUse)
                                    {

                                        concurrentTransactionsPerUser[peUsage.UserName][0]++;
                                        concurrentTransactionsPerService[peUsage.service][0]++;

                                        if (concurrentTransactionsPerUserByTime[peUsage.UserName].ContainsKey(dateTimeString))
                                        {
                                            concurrentTransactionsPerUserByTime[peUsage.UserName][dateTimeString][0]++;
                                        }

                                        if (concurrentTransactionsPerServiceByTime[peUsage.service].ContainsKey(dateTimeString))
                                        {
                                            concurrentTransactionsPerServiceByTime[peUsage.service][dateTimeString][0]++;
                                        }

                                        if (userTypeDictionary.TryGetValue(peUsage.UserName, out string userType))
                                        {

                                            if (userType == "AAM Provider")
                                            {
                                                aamTasks++;
                                            }
                                            else if (userType == "CPM")
                                            {
                                                cpmTasks++;
                                            }
                                            else if (userType == "EPV")
                                            {
                                                epv++;
                                            }
                                            else if (userType == "DR")
                                            {
                                                drTasks++;
                                            }
                                            else if (userType == "Backup")
                                            {
                                                backup++;
                                            }
                                            else if (userType == "PVWA APP")
                                            {
                                                pvwaApp++;
                                            }
                                            else if (userType == "PVWA GW")
                                            {
                                                pvwaGW++;
                                            }
                                            else if (userType == "PSM APP")
                                            {
                                                psmApp++;
                                            }
                                            else if (userType == "PSM GW")
                                            {
                                                psmGW++;
                                            }
                                            else if (userType == "PSM for SSH APP")
                                            {
                                                psmForSshApp++;
                                            }
                                            else if (userType == "PSM for SSH GW")
                                            {
                                                psmForSshGW++;
                                            }
                                            else if (userType == "PSM for SSH ADB")
                                            {
                                                psmForSshADB++;
                                            }
                                            else if (userType == "Builtin")
                                            {
                                                builtin++;
                                            }
                                            else if (userType == "PTA")
                                            {
                                                pta++;
                                            }
                                            else if (userType == "OPM Agent")
                                            {
                                                opmAgent++;
                                            }
                                            else
                                            {
                                                other++;
                                            }
                                        }
                                    }

                                    concurrentVaultTasks.Rows.Add(dateTime, totalTasks, aamTasks, cpmTasks, drTasks, backup, pvwaApp, pvwaGW, psmApp, psmGW, psmForSshApp, psmForSshGW, psmForSshADB, builtin, opmAgent, pta, epv, other);

                                    averageConcurrentTransactionsByUserTypeDictionary["Total"] += totalTasks;
                                    averageConcurrentTransactionsByUserTypeDictionary["AAM Provider"] += aamTasks;
                                    averageConcurrentTransactionsByUserTypeDictionary["CPM"] += cpmTasks;
                                    averageConcurrentTransactionsByUserTypeDictionary["DR"] += drTasks;
                                    averageConcurrentTransactionsByUserTypeDictionary["Backup"] += backup;
                                    averageConcurrentTransactionsByUserTypeDictionary["PVWA APP"] += pvwaApp;
                                    averageConcurrentTransactionsByUserTypeDictionary["PVWA GW"] += pvwaGW;
                                    averageConcurrentTransactionsByUserTypeDictionary["PSM APP"] += psmApp;
                                    averageConcurrentTransactionsByUserTypeDictionary["PSM GW"] += psmGW;
                                    averageConcurrentTransactionsByUserTypeDictionary["PSM for SSH APP"] += psmForSshApp;
                                    averageConcurrentTransactionsByUserTypeDictionary["PSM for SSH GW"] += psmForSshGW;
                                    averageConcurrentTransactionsByUserTypeDictionary["PSM for SSH ADB"] += psmForSshADB;
                                    averageConcurrentTransactionsByUserTypeDictionary["Builtin"] += builtin;
                                    averageConcurrentTransactionsByUserTypeDictionary["OPM Agent"] += opmAgent;
                                    averageConcurrentTransactionsByUserTypeDictionary["PTA"] += pta;
                                    averageConcurrentTransactionsByUserTypeDictionary["EPV"] += epv;
                                    averageConcurrentTransactionsByUserTypeDictionary["Other"] += other;
                                    averageConcurrentTransactionsByUserTypeDictionary["Counter"]++;


                                    if (concurrentTransactionsPerUser[user][0] > concurrentTransactionsPerUser[user][1])
                                    {
                                        concurrentTransactionsPerUser[user][1] = concurrentTransactionsPerUser[user][0];
                                    }

                                    if (concurrentTransactionsPerUserByTime[user][dateTimeString][0] > concurrentTransactionsPerUserByTime[user][dateTimeString][1])
                                    {
                                        concurrentTransactionsPerUserByTime[user][dateTimeString][1] = concurrentTransactionsPerUserByTime[user][dateTimeString][0];
                                    }

                                    if (concurrentTransactionsPerService[serviceName][0] > concurrentTransactionsPerService[serviceName][1])
                                    {
                                        concurrentTransactionsPerService[serviceName][1] = concurrentTransactionsPerService[serviceName][0];
                                    }

                                    if (concurrentTransactionsPerServiceByTime[serviceName][dateTimeString][0] > concurrentTransactionsPerServiceByTime[serviceName][dateTimeString][1])
                                    {
                                        concurrentTransactionsPerServiceByTime[serviceName][dateTimeString][1] = concurrentTransactionsPerServiceByTime[serviceName][dateTimeString][0];
                                    }

                                }

                                if (parts.Length > 5 && DateTime.TryParseExact(parts[0] + " " + parts[1], "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out timeStamp))
                                {

                                    unixTimeMilliseconds = (Int64)(timeStamp.Subtract(new DateTime(1970, 1, 1))).TotalSeconds * 1000 + timeStamp.Millisecond;

                                    if (parts[3] == "START" && line.Contains("]"))
                                    {
                                        userName = parts[6].Split('[')[0];

                                        if (((specificUsersList.Contains(userName.ToLower()))) && !traceDictionary.ContainsKey(parts[5] + parts[6]) && parts[4] == "PE" && Int32.TryParse(parts[5], out ID))
                                        {
                                            traceEntry newEntry = new traceEntry();
                                            newEntry.userName = userName;
                                            newEntry.startTime = timeStamp;
                                            newEntry.startTimeUnix = unixTimeMilliseconds;
                                            newEntry.startPETime = timeStamp;
                                            newEntry.startPETimeUnix = unixTimeMilliseconds;
                                            newEntry.service = serviceName;
                                            newEntry.data = line.Split(']')[1].Replace("ServiceName=" + serviceName, "").TrimStart().TrimStart((char)0x1f).TrimEnd((char)0x1f);
                                            newEntry.ID = ID;
                                            newEntry.IP = "-";

                                            if (specificUsersList.Contains(userName.ToLower()))
                                            {
                                                newEntry.IP = "127.0.0.1";
                                            }

                                            keyCounter++;
                                            newEntry.primaryKey = DateTime.Now.Ticks + keyCounter;
                                            traceDictionary.Add(parts[5] + parts[6], newEntry);
                                        }

                                        if (parts[4] == "PE" && parts.Length > 5 && Int32.TryParse(parts[5], out ID) && traceDictionary.TryGetValue(parts[5] + parts[6], out traceEntry startPeEntry))
                                        {
                                            userName = parts[6].Split('[')[0];
                                            data = line.Split(']')[1].Replace("ServiceName=" + serviceName, "").TrimStart().TrimStart((char)0x1f).TrimEnd((char)0x1f);
                                            startPeEntry.userType = userTypeDictionary[userName];

                                            masterID = ID + parts[6];
                                            subIdKey = parts[5] + parts[6];

                                            if (!subIdDictionary.TryGetValue(masterID, out value))
                                            {
                                                subIdDictionary.Add(masterID, new Dictionary<string, int> { { subIdKey, 0 } });
                                                traceDictionary[masterID].data = data;
                                                traceDictionary[masterID].startPETime = timeStamp;
                                                traceDictionary[masterID].startPETimeUnix = unixTimeMilliseconds;
                                            }
                                            else if (!value.TryGetValue(subIdKey, out int counter))
                                            {
                                                subID = counter + 1;
                                                value.Add(subIdKey, subID);
                                                keyCounter++;

                                                traceDictionary.Add(masterID + subIdKey + "1", new traceEntry(DateTime.Now.Ticks + keyCounter, DateTime.MinValue, 0, timeStamp, unixTimeMilliseconds, ID, subID, serviceName, startPeEntry.userName, startPeEntry.userType, startPeEntry.IP, data));
                                            }
                                            else if (value.ContainsKey(subIdKey))
                                            {
                                                value[subIdKey]++;
                                                keyCounter++;
                                                traceDictionary.Add(masterID + subIdKey + value[subIdKey], new traceEntry(DateTime.Now.Ticks + keyCounter, DateTime.MinValue, 0, timeStamp, unixTimeMilliseconds, ID, value[subIdKey], serviceName, startPeEntry.userName, startPeEntry.userType, startPeEntry.IP, data));
                                            }

                                        }
                                        else if (parts.Length > 4 && Int32.TryParse(parts[4], out ID))
                                        {
                                            if (Regex.IsMatch(parts[parts.Length - 1], @"((^\s*((([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5]))\s*$)|(^\s*((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%.+)?\s*$))"))
                                            {
                                                masterID = ID + parts[5];
                                                userName = line.Split('[')[0].Split(' ').LastOrDefault();

                                                if (!traceDictionary.TryGetValue(masterID, out traceEntry startEntry01))
                                                {
                                                    keyCounter++;
                                                    traceDictionary.Add(masterID, new traceEntry(DateTime.Now.Ticks + keyCounter, timeStamp, unixTimeMilliseconds, ID, 0, serviceName, userName, parts[parts.Length - 1]));
                                                }
                                                else
                                                {

                                                    if (writeToDatabaseTask != null && !writeToDatabaseTask.IsCompleted)
                                                    {
                                                        writeToDatabaseTask.Wait();
                                                    }

                                                    Dictionary<string, traceEntry> cloneDictionary = new Dictionary<string, traceEntry>();
                                                    cloneDictionary = traceDictionary.ToDictionary(entry => entry.Key, entry => entry.Value);
                                                    writeToDatabaseTask = new Task(() => DBFunctions.WriteTraceDictionaryToDatabase(cloneDictionary));
                                                    writeToDatabaseTask.Start();

                                                    traceDictionary.Clear();
                                                    subIdDictionary.Clear();
                                                    traceDictionary = new Dictionary<string, traceEntry>();
                                                    subIdDictionary = new Dictionary<string, Dictionary<string, int>>();
                                                    keyCounter++;
                                                    traceDictionary.Add(masterID, new traceEntry(DateTime.Now.Ticks + keyCounter, timeStamp, unixTimeMilliseconds, ID, 0, serviceName, userName, parts[parts.Length - 1]));
                                                    keyCounter++;
                                                }
                                            }
                                        }
                                    }
                                    else if (parts[3] == "END")
                                    {
                                        if (parts[4] == "PE" && parts.Length > 5 && Int32.TryParse(parts[5], out ID) && traceDictionary.TryGetValue(parts[5] + parts[6], out traceEntry endPeEntry))
                                        {

                                            if (specificUsersList.Contains(endPeEntry.userName.ToLower()))
                                            {
                                                endPeEntry.endTime = timeStamp;
                                                endPeEntry.endTimeUnix = unixTimeMilliseconds;
                                            }

                                            success = "Yes";
                                            subIdKey = parts[5] + parts[6];
                                            masterID = parts[5] + parts[6];

                                            for (int i = parts.Length - 1; i > 5; i--)
                                            {
                                                if (parts[i] == "OK" || parts[i] == "ERR")
                                                {
                                                    index = line.LastIndexOf(parts[i]);
                                                    result = line.Substring(index, line.Length - index);

                                                    if (!result.StartsWith("OK"))
                                                    {
                                                        success = "No";
                                                        result = result.Replace("\"", "").Replace("'", "").Replace("ERR ", "");
                                                        endPeEntry.errorMessage = result;
                                                    }
                                                    else
                                                    {
                                                        outputs = result.Split('=');

                                                        if (outputs.Length == 3)
                                                        {
                                                            result = "";
                                                            if (Int64.TryParse((outputs[1].TrimStart().Split(' ')[0]), out outputSize))
                                                            {
                                                                endPeEntry.outputSize = outputSize;
                                                            }
                                                            if (Int64.TryParse((outputs[2].Trim()), out outputRecords))
                                                            {
                                                                endPeEntry.outputRecords = outputRecords;
                                                            }
                                                        }
                                                    }
                                                    endPeEntry.success = success;
                                                    break;
                                                }
                                            }

                                            value = null;

                                            if (endPeEntry.endPETime == DateTime.MinValue)
                                            {
                                                endPeEntry.endPETime = timeStamp;
                                                endPeEntry.endPETimeUnix = unixTimeMilliseconds;
                                            }

                                            if (subIdDictionary.TryGetValue(masterID, out value))
                                            {
                                                if (value.TryGetValue(subIdKey, out int idValue))
                                                {
                                                    if (idValue > 0 && traceDictionary.TryGetValue(masterID + subIdKey + idValue, out traceEntry entry))
                                                    {
                                                        entry.endPETime = timeStamp;
                                                        entry.endPETimeUnix = unixTimeMilliseconds;
                                                        entry.errorMessage = result;
                                                        entry.outputRecords = outputRecords;
                                                        entry.success = success;

                                                        if (traceDictionary.TryGetValue(masterID + subIdKey + (idValue - 1), out traceEntry entry1))
                                                        {
                                                            entry.startTime = entry1.endPETime;
                                                            entry.startTimeUnix = entry1.endPETimeUnix;
                                                            entry.queueDuration = entry.startPETimeUnix - entry.startTimeUnix;
                                                            entry.execDuration = entry.endPETimeUnix - entry.startPETimeUnix;
                                                            entry.totalQueueTime = entry.queueDuration;
                                                            entry.totalDuration = entry.execDuration;
                                                            traceDictionary[masterID].totalDuration += entry.execDuration;
                                                            traceDictionary[masterID].totalQueueTime += entry.queueDuration;
                                                        }
                                                    }

                                                    if (traceDictionary.TryGetValue(masterID + subIdKey + "1", out traceEntry entry2) && traceDictionary.TryGetValue(masterID, out traceEntry entry3))
                                                    {
                                                        if (entry2.startTime == DateTime.MinValue)
                                                        {
                                                            entry2.startTime = entry3.endPETime;
                                                            entry2.startTimeUnix = entry3.endPETimeUnix;
                                                            entry2.queueDuration = entry2.startPETimeUnix - entry2.startTimeUnix;
                                                            entry2.execDuration = entry2.endPETimeUnix - entry2.startPETimeUnix;
                                                            entry2.totalQueueTime = entry2.queueDuration;
                                                            entry2.totalDuration = entry2.execDuration;
                                                            entry3.totalDuration += entry2.execDuration;
                                                            entry3.totalQueueTime += entry2.queueDuration;
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        else if (Int32.TryParse(parts[4], out ID) && traceDictionary.TryGetValue(parts[4] + parts[5], out traceEntry endEntry))
                                        {
                                            endEntry.endTime = timeStamp;
                                            endEntry.endTimeUnix = unixTimeMilliseconds;
                                            endEntry.queueDuration = endEntry.startPETimeUnix - endEntry.startTimeUnix;
                                            endEntry.execDuration = endEntry.endPETimeUnix - endEntry.startPETimeUnix;
                                            endEntry.totalDuration += endEntry.execDuration;
                                            endEntry.totalQueueTime += endEntry.queueDuration;
                                        }
                                    }
                                }
                            }
                        }

                        traceFileCounter++;

                        fileEndTime = traceDictionary.Last().Value.startPETime;


                        if (traceFileCounter == 4)
                        {

                            if (writeToDatabaseTask != null && !writeToDatabaseTask.IsCompleted)
                            {
                                writeToDatabaseTask.Wait();
                            }
                            Dictionary<string, traceEntry> cloneDictionary = new Dictionary<string, traceEntry>();
                            cloneDictionary = traceDictionary.ToDictionary(entry => entry.Key, entry => entry.Value);
                            writeToDatabaseTask = new Task(() => DBFunctions.WriteTraceDictionaryToDatabase(cloneDictionary));
                            writeToDatabaseTask.Start();

                            traceDictionary.Clear();
                            subIdDictionary.Clear();
                            traceDictionary = new Dictionary<string, traceEntry>();
                            subIdDictionary = new Dictionary<string, Dictionary<string, int>>();


                            maxConcurrentTransactionsByUserTypeTime = (from row in concurrentVaultTasks.AsEnumerable()
                                                                       group row by row.Field<DateTime>("Time").ToString("yyyy-MM-dd HH:mm") into grp
                                                                       select new
                                                                       {
                                                                           Time = DateTime.ParseExact(grp.Key, "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
                                                                           AAMProvider = grp.Max(row => row.Field<int>("AAMProvider")),
                                                                           CPM = grp.Max(row => row.Field<int>("CPM")),
                                                                           DR = grp.Max(row => row.Field<int>("DR")),
                                                                           Backup = grp.Max(row => row.Field<int>("Backup")),
                                                                           PVWAApp = grp.Max(row => row.Field<int>("PVWAAPP")),
                                                                           PVWAGW = grp.Max(row => row.Field<int>("PVWAGW")),
                                                                           PSMAPP = grp.Max(row => row.Field<int>("PSMAPP")),
                                                                           PSMGW = grp.Max(row => row.Field<int>("PSMGW")),
                                                                           PSMforSSHAPP = grp.Max(row => row.Field<int>("PSMforSSHAPP")),
                                                                           PSMforSSHGW = grp.Max(row => row.Field<int>("PSMforSSHGW")),
                                                                           PSMforSSHADB = grp.Max(row => row.Field<int>("PSMforSSHADB")),
                                                                           Builtin = grp.Max(row => row.Field<int>("Builtin")),
                                                                           OPMAgent = grp.Max(row => row.Field<int>("OPMAgent")),
                                                                           PTA = grp.Max(row => row.Field<int>("PTA")),
                                                                           EPV = grp.Max(row => row.Field<int>("EPV")),
                                                                           Other = grp.Max(row => row.Field<int>("Other")),
                                                                       }).ToDataTable();

                            concurrentVaultTasks = (from row in concurrentVaultTasks.AsEnumerable()
                                                    group row by row.Field<DateTime>("Time").ToString("yyyy-MM-dd HH:mm") into grp
                                                    select new
                                                    {
                                                        Time = DateTime.ParseExact(grp.Key, "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture),
                                                        TotalTransactions = grp.Max(row => row.Field<int>("TotalTransactions")),
                                                        AAMProvider = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("AAMProvider")).FirstOrDefault(),
                                                        CPM = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("CPM")).FirstOrDefault(),
                                                        DR = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("DR")).FirstOrDefault(),
                                                        Backup = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Backup")).FirstOrDefault(),
                                                        PVWAAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PVWAAPP")).FirstOrDefault(),
                                                        PVWAGW = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PVWAGW")).FirstOrDefault(),
                                                        PSMAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMAPP")).FirstOrDefault(),
                                                        PSMGw = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMGW")).FirstOrDefault(),
                                                        PSMforSSHAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHAPP")).FirstOrDefault(),
                                                        PSMforSSHGW = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHGW")).FirstOrDefault(),
                                                        PSMforSSHADB = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHADB")).FirstOrDefault(),
                                                        Builtin = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Builtin")).FirstOrDefault(),
                                                        OPMAgent = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("OPMAgent")).FirstOrDefault(),
                                                        PTA = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PTA")).FirstOrDefault(),
                                                        EPV = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("EPV")).FirstOrDefault(),
                                                        Other = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Other")).FirstOrDefault()
                                                    }).ToDataTable();
                            traceFileCounter = 0;

                        }

                    }

                    maxConcurrentTransactionsByUserTypeTime = (from row in concurrentVaultTasks.AsEnumerable()
                                                               group row by row.Field<DateTime>("Time").ToString("yyyy-MM-dd HH:mm") into grp
                                                               select new
                                                               {
                                                                   Time = grp.Key.ToString(),
                                                                   AAMProvider = grp.Max(row => row.Field<int>("AAMProvider")),
                                                                   CPM = grp.Max(row => row.Field<int>("CPM")),
                                                                   DR = grp.Max(row => row.Field<int>("DR")),
                                                                   Backup = grp.Max(row => row.Field<int>("Backup")),
                                                                   PVWAApp = grp.Max(row => row.Field<int>("PVWAAPP")),
                                                                   PVWAGW = grp.Max(row => row.Field<int>("PVWAGW")),
                                                                   PSMAPP = grp.Max(row => row.Field<int>("PSMAPP")),
                                                                   PSMGW = grp.Max(row => row.Field<int>("PSMGW")),
                                                                   PSMforSSHAPP = grp.Max(row => row.Field<int>("PSMforSSHAPP")),
                                                                   PSMforSSHGW = grp.Max(row => row.Field<int>("PSMforSSHGW")),
                                                                   PSMforSSHADB = grp.Max(row => row.Field<int>("PSMforSSHADB")),
                                                                   Builtin = grp.Max(row => row.Field<int>("Builtin")),
                                                                   OPMAgent = grp.Max(row => row.Field<int>("OPMAgent")),
                                                                   PTA = grp.Max(row => row.Field<int>("PTA")),
                                                                   EPV = grp.Max(row => row.Field<int>("EPV")),
                                                                   Other = grp.Max(row => row.Field<int>("Other")),
                                                               }).ToDataTable();



                    maxConcurrentTransactionsByUserTypeTime.Columns["AAMProvider"].ColumnName = "AAM Provider";
                    maxConcurrentTransactionsByUserTypeTime.Columns["OPMAgent"].ColumnName = "OPM Agent";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PVWAApp"].ColumnName = "PVWA APP";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PVWAGW"].ColumnName = "PVWA GW";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PSMApp"].ColumnName = "PSM APP";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PSMGW"].ColumnName = "PSM GW";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PSMForSSHApp"].ColumnName = "PSM for SSH APP";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PSMForSSHGW"].ColumnName = "PSM for SSH GW";
                    maxConcurrentTransactionsByUserTypeTime.Columns["PSMForSSHADB"].ColumnName = "PSM for SSH ADB";

                    maxConcurrentTransactionsByUserTypeTime = removeZeroSumColumns(maxConcurrentTransactionsByUserTypeTime, 1, maxConcurrentTransactionsByUserTypeTime.Columns.Count - 1);

                    maxConcurrentTransactionsByUserType = new DataTable();

                    if (maxConcurrentTransactionsByUserTypeTime != null && maxConcurrentTransactionsByUserTypeTime.Rows.Count > 0 && maxConcurrentTransactionsByUserTypeTime.Columns.Count > 1)
                    {
                        maxConcurrentTransactionsByUserType.Columns.Add("UserType", typeof(string));
                        maxConcurrentTransactionsByUserType.Columns.Add("ConcurrentTransactions", typeof(int));

                        for (int i = 1; i < maxConcurrentTransactionsByUserTypeTime.Columns.Count; i++)
                        {
                            maxConcurrentTransactionsByUserType.Rows.Add(maxConcurrentTransactionsByUserTypeTime.Columns[i].ColumnName, maxConcurrentTransactionsByUserTypeTime.AsEnumerable().Max(x => x.Field<int>(maxConcurrentTransactionsByUserTypeTime.Columns[i].ColumnName)));
                        }

                        maxConcurrentTransactionsByUserType.DefaultView.Sort = "ConcurrentTransactions desc";
                        maxConcurrentTransactionsByUserType = maxConcurrentTransactionsByUserType.DefaultView.ToTable();
                    }

                    concurrentTransactionsPerUser = (from entry in concurrentTransactionsPerUser orderby entry.Value[1] descending select entry).ToDictionary();

                    int count = 0;
                    foreach (var keyValue in concurrentTransactionsPerUser.Keys)
                    {
                        if (!maxTransactionsByUserTime.Columns.Contains(keyValue))
                        {
                            maxTransactionsByUserTime.Columns.Add(keyValue, typeof(int));
                        }


                        for (int i = 0; i < maxTransactionsByUserTime.Rows.Count; i++)
                        {
                            if (keyValue.Trim() != "")
                            {
                                if (concurrentTransactionsPerUserByTime[keyValue].ContainsKey(DateTime.Parse(maxTransactionsByUserTime.Rows[i][0].ToString()).ToString("yyyy-MM-dd HH:mm")))
                                {
                                    maxTransactionsByUserTime.Rows[i][keyValue] = concurrentTransactionsPerUserByTime[keyValue][DateTime.Parse(maxTransactionsByUserTime.Rows[i][0].ToString()).ToString("yyyy-MM-dd HH:mm")][1];
                                }
                                else
                                {
                                    maxTransactionsByUserTime.Rows[i][keyValue] = 0;
                                }
                            }
                        }
                        count++;

                        if (count == 999)
                        {
                            break;
                        }
                    }


                    maxTransactionsByUserTime.DefaultView.Sort = "Time asc";
                    maxTransactionsByUserTime = maxTransactionsByUserTime.DefaultView.ToTable();

                    foreach (KeyValuePair<string, long[]> item in concurrentTransactionsPerUser)
                    {
                        if (userTypeDictionary.TryGetValue(item.Key, out string userType))
                        {
                            maxConcurrentTransactionsByUser.Rows.Add(item.Key, userType, item.Value[1]);
                        }
                        else
                        {
                            maxConcurrentTransactionsByUser.Rows.Add(item.Key, "Unknown", item.Value[1]);
                        }
                    }
                    maxConcurrentTransactionsByUser.DefaultView.Sort = "ConcurrentTransactions desc";
                    maxConcurrentTransactionsByUser = maxConcurrentTransactionsByUser.DefaultView.ToTable();

                    concurrentTransactionsPerService = (from entry in concurrentTransactionsPerService orderby entry.Value[1] descending select entry).ToDictionary();
                    foreach (KeyValuePair<string, long[]> item in concurrentTransactionsPerService)
                    {
                        if (item.Value[1] != 0)
                        {
                            maxConcurrentTransactionsByService.Rows.Add(item.Key, item.Value[1]);
                        }
                    }
                    maxConcurrentTransactionsByService.DefaultView.Sort = "ConcurrentTransactions desc";
                    maxConcurrentTransactionsByService = maxConcurrentTransactionsByService.DefaultView.ToTable();


                    foreach (var keyValue in concurrentTransactionsPerService.Keys)
                    {
                        if (!maxTransactionsByServiceTime.Columns.Contains(keyValue))
                        {
                            maxTransactionsByServiceTime.Columns.Add(keyValue, typeof(int));
                        }

                        for (int i = 0; i < maxTransactionsByServiceTime.Rows.Count; i++)
                        {
                            if (concurrentTransactionsPerServiceByTime[keyValue].ContainsKey(DateTime.Parse(maxTransactionsByServiceTime.Rows[i][0].ToString()).ToString("yyyy-MM-dd HH:mm")))
                            {
                                maxTransactionsByServiceTime.Rows[i][keyValue] = concurrentTransactionsPerServiceByTime[keyValue][DateTime.Parse(maxTransactionsByServiceTime.Rows[i][0].ToString()).ToString("yyyy-MM-dd HH:mm")][1];
                            }
                            else
                            {
                                maxTransactionsByServiceTime.Rows[i][keyValue] = 0;
                            }
                        }
                    }
                    maxTransactionsByServiceTime = removeZeroSumColumns(maxTransactionsByServiceTime, 1, maxTransactionsByServiceTime.Columns.Count - 1);

                    if (writeToDatabaseTask != null && !writeToDatabaseTask.IsCompleted)
                    {
                        writeToDatabaseTask.Wait();
                    }

                    Console.WriteLine(Environment.NewLine + DateTime.Now + " Finalizing writing trace data to local Vault traces SQLite database...");

                    DBFunctions.WriteTraceDictionaryToDatabase(traceDictionary);

                    Console.WriteLine(Environment.NewLine + DateTime.Now + " Determining Vault transactions information...");

                    if (!MainWindow.onlyTraces)
                    {
                        Console.Write(Environment.NewLine);
                    }

                    traceStatistics = new DataTable();
                    cpmSearchTimes = new DataTable();
                    executionTimeByService = new DataTable();
                    executionTimeByService.Columns.Add("Service", typeof(string));
                    executionTimeByService.Columns.Add("Average execution time (sec)", typeof(double));
                    executionTimeByService.Columns.Add("Maximum execution time (sec)", typeof(double));

                    executionTimeByUserType = new DataTable();
                    executionTimeByUserType.Columns.Add("Usertype", typeof(string));
                    executionTimeByUserType.Columns.Add("Average execution time (sec)", typeof(double));
                    executionTimeByUserType.Columns.Add("Maximum execution time (sec)", typeof(double));

                    executionTimeByUser = new DataTable();
                    executionTimeByUser.Columns.Add("Username", typeof(string));
                    executionTimeByUser.Columns.Add("UserType", typeof(string));
                    executionTimeByUser.Columns.Add("Average execution time (sec)", typeof(double));
                    executionTimeByUser.Columns.Add("Maximum execution time (sec)", typeof(double));


                    executionAndQueueTimeDevelopment = new DataTable();
                    executionAndQueueTimeDevelopment.Columns.Add("Average execution time (sec)", typeof(double));
                    executionAndQueueTimeDevelopment.Columns.Add("Average queue time (sec)", typeof(double));
                    transactionsDevelopment = new DataTable();
                    executionAndQueueTimeDevelopment.Columns.Add("Time", typeof(string));
                    transactionsDevelopment.Columns.Add("Time", typeof(string));
                    transactionsDevelopment.Columns.Add("Transactions", typeof(int));
                    transactionsDevelopment.Columns.Add("Successful Transactions", typeof(int));
                    transactionsDevelopment.Columns.Add("Failed Transactions", typeof(int));
                    transactionsByUser = new DataTable();
                    transactionsByUserType = new DataTable();
                    transactionsByService = new DataTable();
                    consumptionByUserType = new DataTable();
                    consumptionByUser = new DataTable();
                    consumptionByService = new DataTable();
                    transactionUsersByUserType = new DataTable();
                    successfulAndFailedTransactions = new DataTable();
                    averageSearchTimeByUser = new DataTable();
                    averageSearchTimeByUserType = new DataTable();
                    cpmSearchTimes = new DataTable();
                    failedTransactionsByUserType = new DataTable();
                    failedTransactionsByUser = new DataTable();
                    failedTransactionsByService = new DataTable();
                    detectedComponentVersions = new DataTable();
                    detectedComponentVersionsSummary = new DataTable();
                    searchOutputRecordsByUser = new DataTable();
                    searchOutputRecordsByUserType = new DataTable();
                    queueTimeByUserType = new DataTable();
                    queueTimeByUser = new DataTable();
                    queueTimeByService = new DataTable();
                    providerCacheRefreshIntervals = new DataTable();
                    DataTable averageCacheRefreshIntervals = new DataTable();

                    tracesStartTime = new DateTime();
                    tracesEndTime = new DateTime();
                    int successfulTransactions = 0;
                    int failedTransactions = 0;
                    int maximumConcurrentTransactions = 0;


                    coveredTransactionMinutes = maxTransactionsByUserTime.Rows.Count;

                    using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\VaultTraces.db; Version = 3;"))
                    {
                        con.Open();

                        using (SQLiteCommand cmd = con.CreateCommand())
                        {

                            cmd.CommandText = "Create INDEX if not exists i_Traces_User ON Traces(User);";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "Create INDEX if not exists i_Traces_Service ON Traces(Service);";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "Create INDEX if not exists i_Traces_UserType ON Traces(UserType);";
                            cmd.ExecuteNonQuery();
                            cmd.CommandText = "Create INDEX if not exists i_Traces_Success ON Traces(Success);";
                            cmd.ExecuteNonQuery();

                            long freeMemory = MainWindow.getFreeMemory();
                            if (freeMemory == 0)
                            {
                                cmd.CommandText = "PRAGMA cache_size = 250000;";
                            }
                            else
                            {
                                cmd.CommandText = "PRAGMA cache_size = " + (int)((freeMemory * 0.9) / 4) + ";";
                            }
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = @"SELECT count(*) as 'Number of searches', round(max(ExecutionTime)/1000.00,2) as 'Maximum search time (sec)', round(avg(ExecutionTime)/1000.00,3) as 'Average search time (sec)', substr( data, instr(DATA, 'PolicyId') + 22, instr(substr(DATA, instr(DATA, 'PolicyId') + 22, 100), '/') - 1) as 'PolicyID', substr( DATA, instr(DATA, 'SafeName='), instr(substr(DATA, instr(DATA, 'SafeName=')), 'FileName=') -2) as 'AllowedSafes' FROM Traces WHERE Service = 'FindFilesServ' and UserType = 'CPM' and DATA LIKE '%PolicyId%Op=ieq%' group by PolicyID order by 3 desc";
                            SQLiteDataAdapter da = new SQLiteDataAdapter(cmd);
                            da.Fill(cpmSearchTimes);

                            cmd.CommandText = @"Select Service, Round(AVG(ExecutionTime)/1000.00,3) as 'Average execution time (sec)', Round(Max(ExecutionTime)/1000.00,3) as 'Maximum execution time (sec)' from traces group by Service order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(executionTimeByService);

                            cmd.CommandText = @"Select UserType, Round(AVG(ExecutionTime)/1000.00,3) as 'Average execution time (sec)', Round(Max(ExecutionTime)/1000.00,3) as 'Maximum execution time (sec)' from traces group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(executionTimeByUserType);

                            cmd.CommandText = @"Select User as UserName, UserType, Round(AVG(ExecutionTime)/1000.00,3) as 'Average execution time (sec)', Round(Max(ExecutionTime)/1000.00,3) as 'Maximum execution time (sec)' from traces group by User order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(executionTimeByUser);

                            cmd.CommandText = @"select count(distinct trim(lower(user))) from traces";
                            transactionUsers = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select UserType, Count(distinct Upper(Trim(User))) as Users from traces group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(transactionUsersByUserType);

                            cmd.CommandText = @"select 'Successful' as Status, Count(Case when Success = 'Yes' Then Success End) as Transactions from traces union all select 'Failed', Count(Case when Success = 'No' Then Success End) as Transactions from traces";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(successfulAndFailedTransactions);

                            cmd.CommandText = @"select DateTime(strftime('%Y-%m-%d %H:%M:%S',min(StartTime))) from traces";
                            tracesStartTime = DateTime.Parse((string)cmd.ExecuteScalar());

                            cmd.CommandText = @"select DateTime(strftime('%Y-%m-%d %H:%M:%S',max(EndTime))) from traces";
                            tracesEndTime = DateTime.Parse((string)cmd.ExecuteScalar());

                            cmd.CommandText = @"select count(*) from traces";
                            totalNumberOfTransactions = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Count(*) from traces where success = 'Yes'";
                            successfulTransactions = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Count(*) from traces where success = 'No'";
                            failedTransactions = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Round(AVG(ExecutionTime)/1000,3) from traces";
                            averageExecutionTime = Convert.ToDouble(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Round(AVG(QueueTime)/1000,3) from traces";
                            averageQueueTime = Convert.ToDouble(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Max(ExecutionTime)/1000 from traces";
                            maximumExecutionTime = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Max(QueueTime)/1000 from traces";
                            maximumQueueTime = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd.CommandText = @"select Round(AVG(ExecutionTime)/1000,3) from traces where service = 'FindFilesServ'";
                            averageSearchTime = Convert.ToDouble(cmd.ExecuteScalar());

                            cmd.CommandText = @"select strftime('%Y-%m-%d %H:%M',(strftime('%s', StartTime) / 60) * 60, 'unixepoch') as 'Time', Round(AVG(ExecutionTime)/1000.00,3) as 'Average execution time (sec)', Round(AVG(QueueTime)/1000.00,3) as 'Average queue time (sec)' from traces group by Time";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(executionAndQueueTimeDevelopment);

                            cmd.CommandText = @"select strftime('%Y-%m-%d %H:%M',(strftime('%s', StartTime) / 60) * 60, 'unixepoch') as 'Time', Count(*) as Transactions, COUNT(case Success when 'Yes' then 1 else null end) as 'Successful Transactions', COUNT(case Success when 'No' then 1 else null end) as 'Failed Transactions' from traces group by Time";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(transactionsDevelopment);

                            cmd.Parameters.AddWithValue("$transcations", totalNumberOfTransactions);
                            cmd.CommandText = @"select User, UserType, Count(*) as Transactions, Round(Count(*) * 100.00 / $transcations,3) as 'Transactions (%)'  from traces group by User order by Transactions desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(transactionsByUser);

                            cmd.Parameters.AddWithValue("$transcations", totalNumberOfTransactions);
                            cmd.CommandText = @"select UserType, Count(*) as Transactions, Round(Count(*) * 100.00 / $transcations,3) as 'Transactions (%)'  from traces group by UserType order by Transactions desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(transactionsByUserType);

                            cmd.Parameters.AddWithValue("$transcations", totalNumberOfTransactions);
                            cmd.CommandText = @"select Service, Count(*) as Transactions, Round(Count(*) * 100.00 / $transcations,3) as 'Transactions (%)' from traces group by Service order by Transactions desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(transactionsByService);

                            cmd.CommandText = "select sum(ExecutionTime) from traces";
                            totalExecutionTime = Convert.ToInt64(cmd.ExecuteScalar());

                            cmd.Parameters.AddWithValue("$totalExecutionTime", totalExecutionTime);
                            cmd.CommandText = @"select UserType, Round(sum(executionTime) / 1000.00 / 60, 4) as 'Consumption (minutes)' , Round(sum(executionTime) * 100.00 / $totalExecutionTime, 4) as 'Consumption (%)' from traces group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(consumptionByUserType);

                            cmd.Parameters.AddWithValue("$totalExecutionTime", totalExecutionTime);
                            cmd.CommandText = @"select User, UserType, Round(sum(executionTime) / 1000.00 / 60, 4) as 'Consumption (minutes)' , Round(sum(executionTime) * 100.00 / $totalExecutionTime, 4) as 'Consumption (%)' from traces group by User order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(consumptionByUser);

                            cmd.Parameters.AddWithValue("$totalExecutionTime", totalExecutionTime);
                            cmd.CommandText = @"select Service, Round(sum(executionTime) / 1000.00 / 60, 4) as 'Consumption (minutes)' , Round(sum(executionTime) * 100.00 / $totalExecutionTime, 4) as 'Consumption (%)' from traces group by Service order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(consumptionByService);

                            cmd.CommandText = @"select User as Username, UserType, Round(Avg(ExecutionTime)/1000,3) as 'Average search time (sec)', Round(Avg(OutputRecords),0) as 'Average output records' from traces where service = 'FindFilesServ' group by Username order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(averageSearchTimeByUser);

                            cmd.CommandText = @"select UserType, Round(Avg(ExecutionTime)/1000,3) as 'Average search time (sec)', Round(Avg(OutputRecords),0) as 'Average output records' from traces where service = 'FindFilesServ' group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(averageSearchTimeByUserType);

                            cmd.CommandText = @"select A.UserType, Failed, Round(Failed * 100.00 / (Failed + B.Successful),3) as 'Failed (%)', B.Successful, Round(B.Successful * 100.00 / (Failed + B.Successful),3) as 'Successful (%)'  from (Select UserType, Count(*) as 'Failed' from traces where Success = 'No' group by UserType) A Left join (Select UserType, Count(*) as 'Successful' from traces where Success = 'Yes' Group By UserType) B on A.UserType = B.UserType Where B.Successful is not null order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(failedTransactionsByUserType);

                            cmd.CommandText = @"select A.User, UserType, Failed, Round(Failed * 100.00 / (Failed + B.Successful),3) as 'Failed (%)', B.Successful, Round(B.Successful * 100.00 / (Failed + B.Successful),3) as 'Successful (%)'  from (Select User, UserType, Count(*) as 'Failed' from traces where Success = 'No' group by User) A Left join (Select User, Count(*) as 'Successful' from traces where Success = 'Yes' Group By User) B on A.User = B.User Where B.Successful is not null order by 4 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(failedTransactionsByUser);

                            cmd.CommandText = @"select A.Service, Failed, Round(Failed * 100.00 / (Failed + B.Successful),3) as 'Failed (%)', B.Successful, Round(B.Successful * 100.00 / (Failed + B.Successful),3) as 'Successful (%)'  from (Select Service, Count(*) as 'Failed' from traces where Success = 'No' group by Service) A Left join (Select Service,Count(*) as 'Successful' from traces where Success = 'Yes' Group By Service) B on A.Service = B.Service Where B.Successful is not null order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(failedTransactionsByService);

                            cmd.Parameters.AddWithValue("$othersType", settings.AllOtherUsers);
                            cmd.CommandText = @"Select UserType as Type, Version, Count(*) as Components from(Select IP, Case When UserType Like 'PSM for SSH%' then 'PSM for SSH' When UserType Like 'PVWA%' Then 'PVWA' When UserType Like 'PSM A%' Then 'PSM' When UserType Like 'PSM G%' Then 'PSM' When Data like '%PIMSU%' then 'OPM Agent' Else UserType End as UserType, substr(data, instr(data, 'Version=') + 8 , instr(data, 'AuthType=') - instr(data, 'Version=') - 9) as Version, data from traces where Service = 'Logon' and data like '%Version=%AuthType=%' Group by 1,2,3) where UserType not in ('EPV','Builtin',$othersType) Group by 1,2 order by 1,3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(detectedComponentVersionsSummary);

                            cmd.Parameters.AddWithValue("$othersType", settings.AllOtherUsers);
                            cmd.CommandText = @"Select UserType as Type, Version, IP from (Select Case When UserType Like 'PSM for SSH%' then 'PSM for SSH' When UserType Like 'PVWA%' Then 'PVWA' When UserType Like 'PSM A%' Then 'PSM' When UserType Like 'PSM G%' Then 'PSM' When Data like '%PIMSU%' then 'OPM Agent' Else UserType End as UserType, IP, substr(data, instr(data, 'Version=') + 8 , instr(data, 'AuthType=') - instr(data, 'Version=') - 9) as Version, data from traces where Service = 'Logon' and data like '%Version=%AuthType=%') where UserType not in ('EPV','Builtin',$othersType) group by IP order by 1,2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(detectedComponentVersions);

                            cmd.CommandText = @"Select User, UserType, Round(Avg(OutputRecords),0) as 'Average output records', Round(Max(OutputRecords),0) as 'Maximum output records', Count(*) as Searches, Sum(OutputRecords) as 'Total output records sum' from traces where service = 'FindFilesServ' Group by User order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(searchOutputRecordsByUser);

                            cmd.CommandText = @"Select UserType, Round(Avg(OutputRecords),0) as 'Average output records', Round(Max(OutputRecords),0) as 'Maximum output records', Count(*) as Searches, Sum(OutputRecords) as 'Total output records sum' from traces where service = 'FindFilesServ' Group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(searchOutputRecordsByUserType);

                            cmd.CommandText = @"Select UserType, Round(Avg(QueueTime) / 1000.00 ,4) as 'Average queue time (sec)', Round(Max(QueueTime)/1000.00,4) as 'Maximum queue time (sec)' from traces Group by UserType order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(queueTimeByUserType);

                            cmd.CommandText = @"Select User, UserType, Round(Avg(QueueTime) / 1000.00 ,4) as 'Average queue time (sec)', Round(Max(QueueTime)/1000.00,4) as 'Maximum queue time (sec)' from traces Group by User order by 3 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(queueTimeByUser);

                            cmd.CommandText = @"Select Service, Round(Avg(QueueTime) / 1000.00 ,4) as 'Average queue time (sec)', Round(Max(QueueTime)/1000.00,4) as 'Maximum queue time (sec)' from traces Group by Service order by 2 desc";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(queueTimeByService);

                            cmd.Parameters.AddWithValue("$coveredMinutes", coveredTransactionMinutes);
                            cmd.CommandText = @"Select User, UserType, IP, Count(*) as 'Cache refreshes', Round($coveredMinutes * 60.00 /  Count(*),0) as 'Refresh interval (seconds)', Round(AVG(OutputRecords),0) as 'Average output records' from traces where UserType in ('AAM Provider','OPM Agent') and Service = 'FindFilesServ' group by User";
                            da = new SQLiteDataAdapter(cmd);
                            da.Fill(providerCacheRefreshIntervals);

                        }
                        con.Close();
                    }

                    if (isNotNullOrEmpty(providerCacheRefreshIntervals))
                    {

                        averageCacheRefreshIntervals = (from p in providerCacheRefreshIntervals.AsEnumerable()
                                                        group p by p.Field<string>("UserType") into d
                                                        select new
                                                        {
                                                            UserType = d.Key,
                                                            AverageCacheRefreshInterval = Math.Round(d.Average(p => Convert.ToInt32(p.Field<Double>("Refresh interval (seconds)"))), 0)
                                                        }).ToDataTable();

                        averageCacheRefreshIntervals.DefaultView.Sort = "UserType";
                        averageCacheRefreshIntervals = averageCacheRefreshIntervals.DefaultView.ToTable();
                    }


                    tracesTimeSpan = new TimeSpan();
                    tracesTimeSpan = tracesEndTime.Subtract(tracesStartTime);

                    string timeSpanString = string.Empty;
                    if (tracesTimeSpan.Days > 0)
                    {
                        if (tracesTimeSpan.Days == 1)
                        {
                            timeSpanString += "1 day ";
                        }
                        else
                        {
                            timeSpanString += tracesTimeSpan.Days + " days ";
                        }
                    }
                    if (tracesTimeSpan.Hours == 1)
                    {
                        timeSpanString += "1 hour ";
                    }
                    else
                    {
                        timeSpanString += tracesTimeSpan.Hours + " hours ";
                    }

                    if (tracesTimeSpan.Minutes == 1)
                    {
                        timeSpanString += "1 minute ";
                    }
                    else
                    {
                        timeSpanString += tracesTimeSpan.Minutes + " minutes";
                    }


                    concurrentVaultTasks = (from row in concurrentVaultTasks.AsEnumerable()
                                            group row by row.Field<DateTime>("Time").ToString("yyyy-MM-dd HH:mm") into grp
                                            select new
                                            {
                                                Time = grp.Key.ToString(),
                                                TotalTransactions = grp.Max(row => row.Field<int>("TotalTransactions")),
                                                AAMProvider = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("AAMProvider")).FirstOrDefault(),
                                                CPM = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("CPM")).FirstOrDefault(),
                                                DR = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("DR")).FirstOrDefault(),
                                                Backup = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Backup")).FirstOrDefault(),
                                                PVWAAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PVWAAPP")).FirstOrDefault(),
                                                PVWAGW = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PVWAGW")).FirstOrDefault(),
                                                PSMAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMAPP")).FirstOrDefault(),
                                                PSMGw = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMGW")).FirstOrDefault(),
                                                PSMforSSHAPP = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHAPP")).FirstOrDefault(),
                                                PSMforSSHGW = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHGW")).FirstOrDefault(),
                                                PSMforSSHADB = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PSMforSSHADB")).FirstOrDefault(),
                                                Builtin = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Builtin")).FirstOrDefault(),
                                                OPMAgent = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("OPMAgent")).FirstOrDefault(),
                                                PTA = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("PTA")).FirstOrDefault(),
                                                EPV = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("EPV")).FirstOrDefault(),
                                                Other = grp.MaxBy(row => row.Field<int>("TotalTransactions")).Select(x => x.Field<int>("Other")).FirstOrDefault()
                                            }).ToDataTable();


                    averageConcurrentTransactionsByUserType = new DataTable();
                    averageConcurrentTransactionsByUserType.Columns.Add("UserType", typeof(string));
                    averageConcurrentTransactionsByUserType.Columns.Add("AverageConcurrentTransactions", typeof(double));
                    foreach (KeyValuePair<string, long> entry in averageConcurrentTransactionsByUserTypeDictionary)
                    {
                        if (entry.Value > 0 && entry.Key != "Counter")
                        {
                            averageConcurrentTransactionsByUserType.Rows.Add(entry.Key, roundNumber(entry.Value * 100.00 / averageConcurrentTransactionsByUserTypeDictionary["Counter"] / 100.00));
                        }
                    }
                    averageConcurrentTransactionsByUserType.DefaultView.Sort = "AverageConcurrentTransactions desc";
                    averageConcurrentTransactionsByUserType = averageConcurrentTransactionsByUserType.DefaultView.ToTable();





                    maximumConcurrentTransactions = concurrentVaultTasks.AsEnumerable().Max(x => x.Field<int>("TotalTransactions"));


                    concurrentVaultTasks.Columns["PVWAApp"].ColumnName = "PVWA APP";
                    concurrentVaultTasks.Columns["PVWAGW"].ColumnName = "PVWA GW";
                    concurrentVaultTasks.Columns["PSMApp"].ColumnName = "PSM APP";
                    concurrentVaultTasks.Columns["PSMGW"].ColumnName = "PSM GW";
                    concurrentVaultTasks.Columns["PSMForSSHApp"].ColumnName = "PSM for SSH APP";
                    concurrentVaultTasks.Columns["PSMForSSHGW"].ColumnName = "PSM for SSH GW";
                    concurrentVaultTasks.Columns["PSMForSSHADB"].ColumnName = "PSM for SSH ADB";
                    concurrentVaultTasks.Columns["AAMProvider"].ColumnName = "AAM Provider";
                    concurrentVaultTasks.Columns["OPMAgent"].ColumnName = "OPM Agent";

                    concurrentVaultTasks.DefaultView.Sort = "Time asc";
                    concurrentVaultTasks = concurrentVaultTasks.DefaultView.ToTable();

                    concurrentVaultTasks = removeZeroSumColumns(concurrentVaultTasks, 1, concurrentVaultTasks.Columns.Count - 1);


                    traceStatistics.Columns.Add("Description", typeof(string));
                    traceStatistics.Columns.Add("Statistic", typeof(string));
                    traceStatistics.Columns.Add("Comment", typeof(string));

                    traceStatistics.Rows.Add("Transactions start time", tracesStartTime, "Start time of the first transaction from the processed Vault trace files");
                    traceStatistics.Rows.Add("Transactions end time", tracesEndTime, "End time of the last transaction from the processed Vault trace files");
                    traceStatistics.Rows.Add("Transactions timespan", timeSpanString, "Timespan of the processed transactions (delta between start time and end time)");

                    if (!timeGapsFound)
                    {
                        traceStatistics.Rows.Add("Covered timespan", "100%", "Covered timespan from processed trace files of entire transactions timespan");
                    }
                    else
                    {
                        traceStatistics.Rows.Add("Covered timespan", Math.Round(coveredTransactionMinutes * 100.00 / tracesTimeSpan.TotalMinutes, 2).ToString() + "%", "Covered timespan from processed trace files of entire transactions timespan");
                    }

                    traceStatistics.Rows.Add("Transactions", string.Format("{0:#,##0}", totalNumberOfTransactions), "Total number of transactions within the transactions timespan");
                    traceStatistics.Rows.Add("Successful transactions", string.Format("{0:#,##0}", successfulTransactions), Math.Round(successfulTransactions * 100.00 / totalNumberOfTransactions, 2) + " % of all transactions");
                    traceStatistics.Rows.Add("Failed transactions", string.Format("{0:#,##0}", failedTransactions), Math.Round(failedTransactions * 100.00 / totalNumberOfTransactions, 2) + " % of all transactions");

                    if (!timeGapsFound)
                    {
                        traceStatistics.Rows.Add("Average transactions per minute", string.Format("{0:#,##0}", totalNumberOfTransactions / tracesTimeSpan.TotalMinutes), "Average transactions per minute in covered timespan");
                    }
                    else
                    {
                        traceStatistics.Rows.Add("Average transactions per minute", string.Format("{0:#,##0}", totalNumberOfTransactions / coveredTransactionMinutes), "Average transactions per minute in covered timespan");
                    }


                    traceStatistics.Rows.Add("Average execution time (sec)", averageExecutionTime, "Average execution time of all transactions in seconds");

                    if (maximumExecutionTime > 1000)
                    {
                        traceStatistics.Rows.Add("Maximum execution time (sec)", string.Format("{0:#,##0}", maximumExecutionTime), "Maximum execution time of a single transaction in seconds");
                    }
                    else
                    {
                        traceStatistics.Rows.Add("Maximum execution time (sec)", maximumExecutionTime, "Maximum execution time of a single transaction in seconds");
                    }

                    traceStatistics.Rows.Add("Average queue time (sec)", averageQueueTime, "Average queue time of all transactions in seconds");

                    if (maximumQueueTime > 1000)
                    {
                        traceStatistics.Rows.Add("Maximum queue time (sec)", string.Format("{0:#,##0}", maximumQueueTime), "Maximum queue time of a single transaction in seconds");
                    }
                    else
                    {
                        traceStatistics.Rows.Add("Maximum queue time (sec)", maximumQueueTime, "Maximum queue time of a single transaction in seconds");
                    }

                    traceStatistics.Rows.Add("Average search time (sec)", averageSearchTime, "Average execution time for search transactions in seconds");
                    traceStatistics.Rows.Add("Maximum concurrent transactions", maximumConcurrentTransactions, "Maximum concurrent transactions within the transactions timespan");
                    traceStatistics.Rows.Add("Average concurrent transactions", averageConcurrentTransactionsByUserType.Rows[0][1], "Average running transactions within the transactions timespan");
                    traceStatistics.Rows.Add("Distinct users", string.Format("{0:#,##0}", transactionUsers), "Distinct users that triggered transactions");
                    traceStatistics.Rows.Add("Average transactions per user", string.Format("{0:#,##0}", totalNumberOfTransactions / transactionUsers), "Rounded to whole number");

                    if (isNotNullOrEmpty(averageCacheRefreshIntervals))
                    {
                        for (int i = 0; i < averageCacheRefreshIntervals.Rows.Count; i++)
                        {
                            traceStatistics.Rows.Add("Average " + averageCacheRefreshIntervals.Rows[i][0] + " refresh interval", averageCacheRefreshIntervals.Rows[i][1], "Average cache refresh interval in seconds");
                        }
                    }


                    if (!settings.keepVaultTraces && File.Exists(vaultTracesDB))
                    {
                        using (SQLiteConnection con = new SQLiteConnection("Data Source = data\\VaultTraces.db; Version = 3;"))
                        {
                            con.Open();

                            using (SQLiteCommand cmd = con.CreateCommand())
                            {
                                cmd.CommandText = "DROP TABLE IF EXISTS Traces";
                                cmd.ExecuteNonQuery();
                                cmd.CommandText = "VACUUM";
                                cmd.ExecuteNonQuery();
                            }
                            con.Close();
                        }
                    }

                    storeTracesDataTablesInSqliteDatabase();

                    processingTraces = false;

            
                    Console.WriteLine(Environment.NewLine + "Trace analysis duration: " + sw.Elapsed.Minutes + " minutes " + sw.Elapsed.Seconds + " seconds " + sw.Elapsed.Milliseconds + " milliseconds" + Environment.NewLine);
                    
                    
                }
            }
            catch (Exception ex)
            {
                processingTraces = false;
                Console.WriteLine(DateTime.Now + " Error while processing Vault trace files");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException.Message);
            }
        }