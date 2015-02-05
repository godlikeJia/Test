#!/usr/bin/perl -w
###############################################################################
# �ű����ƣ�recal.pl
# ���ܣ����߳���������ջ���
# ʹ�÷�����
#    recal.pl clear
#    recal.pl thread_id
#    recal.pl
# ������clear ��ʾȫ�����Ӷ�����ϵͳ�Ĺ���Ŀ¼�͹�����������֧���Ľӿڱ�
#       thread_id��ʾ��Ҫ�������̺߳�
#       �޲�����ʾʹ���佻��ʽ����������������ղ���
###############################################################################

use warnings;
use Switch;
use POSIX;

sub exec_cmd;
sub main;
sub check_if_directory;

#������־�ļ�
my $run_path = `pwd`;
chomp($run_path);
my $current_date = `date +%Y-%m-%d`;
chomp($current_date);
my $run_log_file;
$run_log_file = $run_path . "/log/recal-" . $current_date . ".log";

if (! -e $run_path . "/log"){
  print "Ŀ¼������. �봴��Ŀ¼: [$run_path/log].\n";
  exit 1;
}

my @FILE_TYPES=("CUST", "ACCT", "USER", "UEXT", "TERM", "PROJ","PROM","ORDR","APAY","PFFA","BUSI","SERV","CARD","RECE","PAID","ARRE","PAYO","USAG","GROU","BALA","NSDF","NPRE","FRZD","T001","T002","T003","T004","T005","C001","C002","CBAL","DBAL");
my @DRIVER_TYPES=("CRUP", "CRPM", "CRPD", "CRBO", "CRCI", "CRCS", "CRPO", "CRYC", "CRCH");
my %FILE_TYPE_INFO=(
'CUST'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400129','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'30','bps_thread_id'=>'1330011810','prov_stat_table','PROV_CUST_INFO'     ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011810', 'fid'=>'018'},
'ACCT'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400126','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'30','bps_thread_id'=>'1330011910','prov_stat_table','PROV_ACCT_INFO'     ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011910', 'fid'=>'019'},
'USER'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400102','shm_proc_id'=>'11','shm_thread_id'=>'01110101','bps_proc_id'=>'31','bps_thread_id'=>'1331010110','prov_stat_table','PROV_USER_INFO'     ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010110', 'fid'=>'001'},
'UEXT'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400112','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010210','prov_stat_table','PROV_USER_EXTINFO'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010210', 'fid'=>'002'},
'TERM'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400113','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010310','prov_stat_table','PROV_USER_TERMINAL' ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010310', 'fid'=>'003'},
'PROJ'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400114','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010410','prov_stat_table','PROV_USER_PROJECT'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010410', 'fid'=>'004'},
'PROM'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400115','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010510','prov_stat_table','PROV_USER_PROMOTION','prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010510', 'fid'=>'005'},
'ORDR'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400106','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010610','prov_stat_table','PROV_PROD_ORDER'    ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010610', 'fid'=>'006'},
'APAY'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400127','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010710','prov_stat_table','PROV_ADVANCED_PAY'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010710', 'fid'=>'007'},
'PFFA'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400116','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010810','prov_stat_table','PROV_PAYFEE_AGENT'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010810', 'fid'=>'008'},
'BUSI'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400117','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332010910','prov_stat_table','PROV_BUSINESS_FEE'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415010910', 'fid'=>'009'},
'SERV'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400118','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011010','prov_stat_table','PROV_SERVICE'       ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011010', 'fid'=>'010'},
'CARD'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400119','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011110','prov_stat_table','PROV_CARD_SALE'     ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011110', 'fid'=>'011'},
'RECE'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400128','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011210','prov_stat_table','PROV_RECEIVABLES'   ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011210', 'fid'=>'012'},
'PAID'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400120','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011310','prov_stat_table','PROV_PAID_UP'       ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011310', 'fid'=>'013'},
'ARRE'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400121','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011410','prov_stat_table','PROV_ARREARS_INFO'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011410', 'fid'=>'014'},
'PAYO'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400122','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011510','prov_stat_table','PROV_PAY_OFF'       ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011510', 'fid'=>'015'},
'USAG'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400123','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011610','prov_stat_table','PROV_SERVICE_USAGE' ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011610', 'fid'=>'016'},
'GROU'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400124','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332011710','prov_stat_table','PROV_GROUP_INFO'    ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415011710', 'fid'=>'017'},
'BALA'=>{'fts_proc_id'=>'40','fts_thread_id'=>'11400125','shm_proc_id'=>'12','shm_thread_id'=>'01120101','bps_proc_id'=>'32','bps_thread_id'=>'1332012010','prov_stat_table','PROV_BALANCE_INFO'  ,'prov_stat_proc_id'=>'15','prov_stat_thread_id'=>'1415012010', 'fid'=>'020'},
'NSDF'=>{'fts_proc_id'=>'20','fts_thread_id'=>'11200101','shm_proc_id'=>'60','shm_thread_id'=>'01600101','bps_proc_id'=>'50','bps_thread_id'=>'1350010110','prov_stat_table',''                   ,'prov_stat_proc_id'=>'20','prov_stat_thread_id'=>'1420010110', 'fid'=>'021'},
'NPRE'=>{'fts_proc_id'=>'','fts_thread_id'=>'','shm_proc_id'=>'','shm_thread_id'=>'','bps_proc_id'=>'','bps_thread_id'=>'','prov_stat_table',''                   ,'prov_stat_proc_id'=>'21','prov_stat_thread_id'=>'1421010110', 'fid'=>'022'},
'FRZD'=>{'fts_proc_id'=>'',  'fts_thread_id'=>'',        'shm_proc_id'=>'11','shm_thread_id'=>'01110101','bps_proc_id'=>'90','bps_thread_id'=>'1390010110','prov_stat_table',''                   ,'prov_stat_proc_id'=>'',  'prov_stat_thread_id'=>''          , 'fid'=>''},
'T001'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010110','prov_stat_table','PROV_T001'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010110', 'fid'=>'T001'},
'T002'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010210','prov_stat_table','PROV_T002'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010210', 'fid'=>'T002'},
'T003'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010310','prov_stat_table','PROV_T003'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010310', 'fid'=>'T003'},
'T004'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010410','prov_stat_table','PROV_T004'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010410', 'fid'=>'T004'},
'T005'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010510','prov_stat_table','PROV_T005'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010510', 'fid'=>'T005'},
'C001'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010610','prov_stat_table','PROV_C001'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010610', 'fid'=>'C001'},
'C002'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010710','prov_stat_table','PROV_C002'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010710', 'fid'=>'C002'},
'CBAL'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010810','prov_stat_table','PROV_CBAL'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010810', 'fid'=>'CBAL'},
'DBAL'=>{'fts_proc_id'=>'42','fts_thread_id'=>'11420101','shm_proc_id'=>'61','shm_thread_id'=>'01610101','bps_proc_id'=>'70','bps_thread_id'=>'1370010910','prov_stat_table','PROV_DBAL'          ,'prov_stat_proc_id'=>'16','prov_stat_thread_id'=>'1416010910', 'fid'=>'DBAL'}
);

my %THREAD_INFO = (
'11400101'=>{'log_table'=>'LOG_FTS_PROV'},
'11400102'=>{'log_table'=>'LOG_FTS_PROV'},
'11400103'=>{'log_table'=>'LOG_FTS_PROV'},
'11400104'=>{'log_table'=>'LOG_FTS_PROV'},
'11400105'=>{'log_table'=>'LOG_FTS_PROV'},
'11400106'=>{'log_table'=>'LOG_FTS_PROV'},
'11400113'=>{'log_table'=>'LOG_FTS_PROV'},
'11400114'=>{'log_table'=>'LOG_FTS_PROV'},
'11400115'=>{'log_table'=>'LOG_FTS_PROV'},
'11400116'=>{'log_table'=>'LOG_FTS_PROV'},
'11400117'=>{'log_table'=>'LOG_FTS_PROV'},
'11400118'=>{'log_table'=>'LOG_FTS_PROV'},
'11400119'=>{'log_table'=>'LOG_FTS_PROV'},
'11400120'=>{'log_table'=>'LOG_FTS_PROV'},
'11400121'=>{'log_table'=>'LOG_FTS_PROV'},
'11400122'=>{'log_table'=>'LOG_FTS_PROV'},
'11400123'=>{'log_table'=>'LOG_FTS_PROV'},
'11400124'=>{'log_table'=>'LOG_FTS_PROV'},
'11400125'=>{'log_table'=>'LOG_FTS_PROV'},
'11400126'=>{'log_table'=>'LOG_FTS_PROV'},
'11420101'=>{'log_table'=>'log_fts_prov'},
'11400112'=>{'log_table'=>'log_fts_prov'},
'11400127'=>{'log_table'=>'log_fts_prov'},
'11400128'=>{'log_table'=>'log_fts_prov'},
'11400129'=>{'log_table'=>'log_fts_prov'},
'11400151'=>{'log_table'=>'LOG_FTS_PROV'},
'11400152'=>{'log_table'=>'LOG_FTS_PROV'},
'11400153'=>{'log_table'=>'LOG_FTS_PROV'},
'11400154'=>{'log_table'=>'LOG_FTS_PROV'},
'11400155'=>{'log_table'=>'LOG_FTS_PROV'},
'11400156'=>{'log_table'=>'LOG_FTS_PROV'},
'11400157'=>{'log_table'=>'LOG_FTS_PROV'},
'11400158'=>{'log_table'=>'LOG_FTS_PROV'},
'11400159'=>{'log_table'=>'LOG_FTS_PROV'},
'11400160'=>{'log_table'=>'LOG_FTS_PROV'},
'11400161'=>{'log_table'=>'LOG_FTS_PROV'},
'11400162'=>{'log_table'=>'LOG_FTS_PROV'},
'11400163'=>{'log_table'=>'LOG_FTS_PROV'},
'11400164'=>{'log_table'=>'LOG_FTS_PROV'},
'11400165'=>{'log_table'=>'LOG_FTS_PROV'},
'11400166'=>{'log_table'=>'LOG_FTS_PROV'},
'11400167'=>{'log_table'=>'LOG_FTS_PROV'},
'11400168'=>{'log_table'=>'LOG_FTS_PROV'},
'11400169'=>{'log_table'=>'LOG_FTS_PROV'},
'11400170'=>{'log_table'=>'LOG_FTS_PROV'},

'11400201'=>{'log_table'=>'LOG_FTS_PROV'},
'11400202'=>{'log_table'=>'LOG_FTS_PROV'},
'11400203'=>{'log_table'=>'LOG_FTS_PROV'},
'11400204'=>{'log_table'=>'LOG_FTS_PROV'},
'11400205'=>{'log_table'=>'LOG_FTS_PROV'},
'11400206'=>{'log_table'=>'LOG_FTS_PROV'},
'11400207'=>{'log_table'=>'LOG_FTS_PROV'},
'11400208'=>{'log_table'=>'LOG_FTS_PROV'},
'11400209'=>{'log_table'=>'LOG_FTS_PROV'},
'11400210'=>{'log_table'=>'LOG_FTS_PROV'},
'11400211'=>{'log_table'=>'LOG_FTS_PROV'},
'11400212'=>{'log_table'=>'LOG_FTS_PROV'},
'11400213'=>{'log_table'=>'LOG_FTS_PROV'},
'11400214'=>{'log_table'=>'LOG_FTS_PROV'},
'11400215'=>{'log_table'=>'LOG_FTS_PROV'},
'11400216'=>{'log_table'=>'LOG_FTS_PROV'},
'11400217'=>{'log_table'=>'LOG_FTS_PROV'},
'11400218'=>{'log_table'=>'LOG_FTS_PROV'},
'11400219'=>{'log_table'=>'LOG_FTS_PROV'},
'11400220'=>{'log_table'=>'LOG_FTS_PROV'},
'11400251'=>{'log_table'=>'LOG_FTS_PROV'},
'11400252'=>{'log_table'=>'LOG_FTS_PROV'},
'11400253'=>{'log_table'=>'LOG_FTS_PROV'},
'11400254'=>{'log_table'=>'LOG_FTS_PROV'},
'11400255'=>{'log_table'=>'LOG_FTS_PROV'},
'11400256'=>{'log_table'=>'LOG_FTS_PROV'},
'11400257'=>{'log_table'=>'LOG_FTS_PROV'},
'11400258'=>{'log_table'=>'LOG_FTS_PROV'},
'11400259'=>{'log_table'=>'LOG_FTS_PROV'},
'11400260'=>{'log_table'=>'LOG_FTS_PROV'},
'11400261'=>{'log_table'=>'LOG_FTS_PROV'},
'11400262'=>{'log_table'=>'LOG_FTS_PROV'},
'11400263'=>{'log_table'=>'LOG_FTS_PROV'},
'11400264'=>{'log_table'=>'LOG_FTS_PROV'},
'11400265'=>{'log_table'=>'LOG_FTS_PROV'},
'11400266'=>{'log_table'=>'LOG_FTS_PROV'},
'11400267'=>{'log_table'=>'LOG_FTS_PROV'},
'11400268'=>{'log_table'=>'LOG_FTS_PROV'},
'11400269'=>{'log_table'=>'LOG_FTS_PROV'},
'11400270'=>{'log_table'=>'LOG_FTS_PROV'},

'11200101'=>{'log_table'=>'log_fts_ns'}
);

my %DRIVER_TYPE_INFO=(
'CRUP'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910010110','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930010110','output_stat_dir'=>'SEUI','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010110','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010110'},
'CRPM'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910012010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930012010','output_stat_dir'=>'SEPM','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010210','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010210'},
'CRPD'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910013010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930013010','output_stat_dir'=>'SEPD','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010310','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010310'},
'CRBO'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910014010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930014010','output_stat_dir'=>'SEBO','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010410','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010410'},
'CRCI'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910016010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930016010','output_stat_dir'=>'SECI','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010510','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010510'},
'CRCS'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910017010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930017010','output_stat_dir'=>'SECS','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010610','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010610'},
'CRPO'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910018010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930018010','output_stat_dir'=>'YHCP','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010710','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010710'},
'CRYC'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910019010','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930019010','output_stat_dir'=>'YHYC','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010810','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010810'},
'CRCH'=>{'unipre_shm_proc_id'=>'20','unipre_shm_thread_id'=>'01200101','unipre_proc_id'=>'10','unipre_thread_id'=>'1910019910','unical_shm_proc_id'=>'40','unical_shm_thread_id'=>'01400101','unical_proc_id'=>'30','unical_thread_id'=>'1930019910','output_stat_dir'=>'CHNL','comm_stat_proc_id'=>'50','comm_stat_thread_id'=>'1450010910','sett_stat_proc_id'=>'80','sett_stat_thread_id'=>'1480010910'}
);

my %ALL_DIR=(
'bps'=>   ('ACCT','APAY','ARRE','BALA','BUSI','C001','C002','CARD','CBAL','CMAP','CUST','DBAL','ERP1','ERP2',
           'GROU','KPXS','MMAP','NSDF','NSSF','ORDR','PAID','PAYO','PBIL','PCDR','PFFA','PROJ','PROM','RECE',
           'SERV','T001','T002','T003','T004','T005','TERM','UEXT','USAG','USER','USRD'),
'busi'=>  ('test'),
'exp'=>   ('RPROV', 'DOWM'),
'fts'=>   ('PROV','ns'),
'shm'=>   ('0110','0111','0112','0120','0130','0140','0150','0160','0161','0170','0171','0172','0173','0180'),
'stat'=>  ('AACC','CARDD','CHNL','ERP1','NPRE','NSDF','NSSF','PACCT','PAPAY','PARRE','PBALA','PBUSI',
           'PC001','PC002','PCARD','PCBAL','PCMAP','PCUST','PDBAL','PGROU','PKPXS','PMMAP','PORDR',
           'PPAID','PPAYO','PPBIL','PPCDR','PPFFA','PPROJ','PPROM','PRECE','PSERV','PT001','PT002',
           'PT003','PT004','PT005','PTERM','PUEXT','PUSAG','PUSER','REPO','SEBO','SECI','SECS','SEPD',
           'SEPM','SEUI','SSFD','USERD','YHCP','YHYC'),
'unical'=>('CRUP', 'CRPM', 'CRPD', 'CRBO', 'CRCI', 'CRCS', 'CRPO', 'CRYC', 'CRCH')
);

my @shm_seq=('01110101','01100101','01120101','01200101','01300101','01400101');
my $QUERY_USER = $ENV{'QUERY_USER'};

if(not defined $QUERY_USER or $QUERY_USER eq ""){
	print "��������δ����. [QUERY_USER]\n";
	exit 1;
}
my $QUERY_PASSWD = $ENV{'QUERY_PASSWD'};
if(not defined $QUERY_PASSWD or $QUERY_PASSWD eq ""){
	print "��������δ����. [QUERY_PASSWD]\n";
	exit 1;
}
my $QUERY_TNS = $ENV{'QUERY_TNS'};
if(not defined $QUERY_TNS or $QUERY_TNS eq ""){
	print "��������δ����. [QUERY_TNS]\n";
	exit 1;
}

my $CONN_STR = "$QUERY_USER/$QUERY_PASSWD\@$QUERY_TNS";

my $UNICAL_PROVINCE_CODE = "$ENV{'UNICAL_PROVINCE_CODE'}";
if(not defined $UNICAL_PROVINCE_CODE or $UNICAL_PROVINCE_CODE eq ""){
	print "��������δ����. [UNICAL_PROVINCE_CODE]\n";
	exit 1;
}

my $UNICAL_DT = "$ENV{'UNICAL_DT'}";
if(not defined $UNICAL_DT or $UNICAL_DT eq ""){
	print "��������δ����. [UNICAL_DT]\n";
	exit 1;
}

my $HOME = "$ENV{'AIOSS_HOME'}";
if(not defined $HOME or $HOME eq ""){
	print "��������δ����. [AIOSS_HOME]\n";
	exit 1;
}

my $ACCT_CYCLE = "$ENV{'ACCT_CYCLE'}";
if(not defined $ACCT_CYCLE or $ACCT_CYCLE eq ""){
	print "��������δ����. [ACCT_CYCLE]\n";
	exit 1;
}

my $RELEASE_BIN_PATH = "$HOME/debug/bin";
my $DEBUG_BIN_PATH = "$HOME/debug/bin";
my $HOST_ID = "$ENV{'UNICAL_HOST_ID'}";
if(not defined $HOST_ID or $HOST_ID eq ""){
	print "��������δ����. [UNICAL_HOST_ID]\n";
	exit 1;
}

chomp(my $COMM_CYCLE = `date +%Y-%m-%d`);
chomp(my $USER_NAME = `whoami`);
chomp(my $HOST_NAME = `hostname`);

my $globle_index = 0;

my $TUI_THREAD_FLAG = 0;
my $TUI_THREAD;
my $TUI_MODULE;
my $TUI_PROCESS;

#����������
&main;

##################################################
#   ������main
#   ���ܣ�recal.pl ���������
##################################################
sub main
{
	if(defined $ARGV[0]){
		&tui_main;
	}else{
		&interactive_main;
	}
}

##################################################
# ������tui_main
# ���ܣ�recal.pl�����̺߳���Ϊ��������Ϊtui���õ�������
#   ���̺߳���'clear'�����ʱ�򣬱�ʾȫ��ղ���   
##################################################
sub tui_main
{
	$TUI_THREAD_FLAG = 1;
	$TUI_THREAD = $ARGV[0];
	$TUI_FILE_TYPE = $ARGV[1];
	
	# ��������� clear ����ʾ��TUI���͵���ջ�����ָ��
	if(lc($TUI_THREAD) eq "clear"){
		&clear_all;
		exit 0;
	}
	
	# ������̺߳ţ������ִ��                      
	$TUI_MODULE = substr($TUI_THREAD,0,2);
	$TUI_PROCESS = substr($TUI_THREAD,2,2);
	$TUI_PROC_THREAD = substr($TUI_THREAD,4,4);
	if($TUI_MODULE eq "11"){
		#TUI FTS REDO
		if($TUI_PROCESS eq "40" and $TUI_PROC_THREAD gt "0250"){
			&redo_cbss_fts_single_filetype($TUI_THREAD, $TUI_FILE_TYPE);
		}else{
			&redo_fts_single_filetype($TUI_THREAD, $TUI_FILE_TYPE);
		}
	}elsif($TUI_MODULE eq "13"){
		#TUI BPS REDO
		if($TUI_THREAD eq "13900101"){
      &redo_13900101;
    }else{
		  foreach (@FILE_TYPES){
		  	if(substr($FILE_TYPE_INFO{$_}{'bps_thread_id'}, 0, 8) eq $TUI_THREAD){
		  	  &redo_check_single_filetype($_);
		  	  last; 
		  	}
		  }
	  }
		
	}elsif($TUI_MODULE eq "14" and ($TUI_PROCESS eq "15" or $TUI_PROCESS eq "20" or $TUI_PROCESS eq "21" or $TUI_PROCESS eq "16")){
		#TUI STAT REDO  , DATA 
		foreach (@FILE_TYPES){
			if(substr($FILE_TYPE_INFO{$_}{'prov_stat_thread_id'}, 0, 8) eq $TUI_THREAD){
			  &stat_prov($TUI_THREAD, $_);
			  last; 
			}
		}
	}elsif($TUI_MODULE eq "14" and $TUI_PROCESS eq "50"){
		#TUI STAT REDO, COMM
		foreach (@DRIVER_TYPES){
			if(substr($DRIVER_TYPE_INFO{$_}{'comm_stat_thread_id'}, 0, 8) eq $TUI_THREAD){
			  &stat_comm($_);
			  last; 
			}
		}
		
	}elsif($TUI_MODULE eq "14" and $TUI_PROCESS eq "80"){
		#TUI STAT REDO, SETT
		foreach (@DRIVER_TYPES){
			if(substr($DRIVER_TYPE_INFO{$_}{'sett_stat_thread_id'},0,8) eq $TUI_THREAD){
			  &stat_sett($_);
			  last; 
			}
		}
		
	}elsif($TUI_MODULE eq "19" and $TUI_PROCESS eq "10"){
		#TUI UNICAL REDO
		foreach (@DRIVER_TYPES){
			if(substr($DRIVER_TYPE_INFO{$_}{'unipre_thread_id'}, 0, 8) eq $TUI_THREAD){
			  &redo_unical_single_driver_stage1($_);
			  last; 
			}
		}
	}elsif($TUI_MODULE eq "19" and $TUI_PROCESS eq "30"){
		#TUI UNICAL REDO
		foreach (@DRIVER_TYPES){
			if(substr($DRIVER_TYPE_INFO{$_}{'unical_thread_id'}, 0, 8) eq $TUI_THREAD){
			  &redo_unical_single_driver_stage3($_);
			  last;
			}
		}
	}elsif($TUI_THREAD eq "19200101"){
		#TUI UNICAL REDO
	  &redo_unical_single_driver_stage2("CRUP");
	  
	}elsif($TUI_THREAD eq "14600101"){
		#TUI STAT DR_FRZ REDO
	  &stat_frz;
	  
	}elsif($TUI_THREAD eq "14610101"){
		#TUI STAT DR_FRZ REDO
	  &stat_comp;
	}elsif($TUI_MODULE eq "03"){
	  #Ӷ����ϸ�·�ʡ�ֽӿ�  
    if(index("03250100|03250101|03250102|03250103|03250104|03250105", $TUI_THREAD) >= 0){
      my $sql_id ="$UNICAL_PROVINCE_CODE" ."_del_ART_LTS";
      my $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n���֧����[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼�������Ѿ�ִ��.\n";

      $sql_id ="$UNICAL_PROVINCE_CODE" ."_sel_ART_LTS";
      $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n֧����[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼������[$msg].\n";
      if($msg eq "0") {
        print "���֧����[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼ �ɹ�.\n";
      }else{
        print "���֧����[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼ ʧ��.\n";
      }
    }elsif(index("03700101|03800101|03100101", $TUI_THREAD) >= 0){
      my $sql_id = "$UNICAL_PROVINCE_CODE" ."_del_USG_LTS";
      my $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n��������[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼�������Ѿ�ִ��.\n";

      $sql_id = "$UNICAL_PROVINCE_CODE" ."_sel_USG_LTS";
      $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n�����[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼������[$msg].\n";
      if($msg eq "0") {
        print "��������[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼ �ɹ�.\n";
      }else{
        print "��������[LOG_THREAD_STATE]�б�ʡ[$TUI_THREAD]���ݼ�¼ ʧ��.\n";
      }
      my @cmd_string;
    
     if($TUI_THREAD eq "03700101"){
          @cmd_string = (
          "cd ${UNICAL_DT}/bps/FRZD/in; ls|xargs -i rm -rf {}"
          );
     }elsif ($TUI_THREAD eq "03100101"){
          @cmd_string = (
          "cd ${UNICAL_DT}/unical/CRCH/stage1/in; ls|xargs -i rm -rf {} "
          );
     }else{
	}
      &exec_cmd(@cmd_string);	

    }else{
    
    }


	}else{
		&log("��Ч�� recal.pl ����");
	};
}

##################################################
#   ������interactive_main
#   ���ܣ�recal.pl�ű��Ľ���ʽ���������
##################################################
sub interactive_main
{
  &print_welcome;
  &log("");
  &log("####################��������recal.pl�ű�####################");

  my $choice = 0;
  while(1)
  {
    &print_menu_list("main");
    print "ѡ��\n";
    $choice = <STDIN>;
    chomp($choice);
    
    if($choice eq "1"){&redo_fts;}
    elsif($choice eq "2"){&redo_check;}
    elsif($choice eq "3"){&redo_unical;}
    elsif($choice eq "4"){&redo_stat;}
    elsif($choice eq "5"){&clear_all;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    else{print "��Ч��������ѡ��\n";}
  }
}

##################################################
#   ������print_welcome
#   ���ܣ�recal.pl�ű�����������
##################################################
sub print_welcome
{
  system("clear");
  print "\n";
  print "#############################################################\n";
  print "#                                                           #\n";
  print "#                   Ӷ�����ϵͳ����ű�                    #\n";
  print "#  �ű����ƣ� recal.pl                                      #\n";
  print "#  �ű����ԣ� perl                                          #\n";
  print "#  �ű����ܣ� ʵ��Ӷ�����ϵͳ��ͬ�׶ε����㹦�ܡ�          #\n";
  print "#  ʹ�÷����� �� UNIX/LINUX ������ֱ������ű�����          #\n";
  print "#             �������ɣ�û�������в�����                    #\n";
  print "#                                                           #\n";
  print "#############################################################\n";
  print "\n";
}

##################################################
#   ������log
#   ���ܣ���¼��־�ļ��ĺ���
##################################################
sub log
{
	my $log_time = strftime("[%Y-%m-%d %H:%M:%S]:", localtime);
	my $log_content = shift;
  open  (LOG_FILE, ">>$run_log_file") or die "����־�ļ�ʧ�ܣ�$!\n";
  print LOG_FILE $log_time;
  print LOG_FILE $log_content,"\n";
  close LOG_FILE;
  
  print $log_time;
  $log_content eq ""?print "\n":print $log_content,"\n";
}

##################################################
#   ������truct_table
#   ���ܣ�����ִ��truncate table�洢����
##################################################
sub trunc_table
{
	open  ORA,"| sqlplus -s $CONN_STR " or die "$!\n";
	foreach(@_){
		my $count = &get_sql_result("select count(*) from UTL4_TRUNC_TAB_LIST where upper(table_name) = upper('$_')");
		if(not defined $count){
			print "trunc_table() get_sql_result() error.\n";
			close ORA;
			exit 1;
		}
		&exec_sql("insert into UTL4_TRUNC_TAB_LIST (TABLE_NAME,MEMO,TRUNC_TYPE) Values ('$_','RECAL','T');") if ($count == 0);
		&log("��SQL������truncate table����$_��");
		my $trunc_string="set serveroutput on
		                Declare
                    Ret_Code    Number(5);
                    ret_message varchar2(4000);
                    Begin
                    UTL4_TRUNC_TAB_SP('$_', Ret_Code, ret_message );
                    dbms_output.PUT_LINE(ret_code||chr(10)||ret_message);
                    end;
                    /";
    print ORA "$trunc_string\n";
	}
  close ORA;
  &log("��SQL�������Ͽ����ݿ�����");
}

##################################################
#   ������truct_table_partition
#   ���ܣ�����ִ��truncate table�洢���̣��������
##################################################
sub truct_table_partition
{
	open  ORA,"| sqlplus -s $CONN_STR " or die "$!\n";
	foreach(@_){
		my $count = &get_sql_result("select count(*) from UTL4_TRUNC_TAB_LIST where upper(table_name) = upper('$_')");
		if(not defined $count){
			print "trunc_table() get_sql_result() error.\n";
			close ORA;
			exit 1;
		}
		&exec_sql("insert into UTL4_TRUNC_TAB_LIST (TABLE_NAME,MEMO,TRUNC_TYPE) Values ('$_','RECAL','P');") if ($count == 0);
		&log("��SQL������truncate table����$_��");
		my $trunc_string="set serveroutput on
		  							Declare
                    Ret_Code    Number(5);
                    ret_message varchar2(4000);
                    Begin
                    UTL4_TRUNC_TAB_SP('$_', Ret_Code, ret_message, 'P_${UNICAL_PROVINCE_CODE}');
                    dbms_output.PUT_LINE(ret_code||chr(10)||ret_message);
                    end;
                    /";
    print ORA "$trunc_string\n";
	}
  close ORA;
  &log("��SQL�������Ͽ����ݿ�����");
}

##################################################
#   ������exec_sql
#   ���ܣ�ִ�����ݿ�����ĺ���
##################################################
sub exec_sql
{
  #&log("��SQL��������ʼ���ݿ����ӣ����Ӵ�����$CONN_STR��");
  open  ORA,"| sqlplus -s $CONN_STR " or die "$!\n";
  foreach (@_)
  {
    &log("��SQL������SQL��䣺��$_��");
    print ORA "$_\n";
  }
  print ORA "commit;\n";
  print ORA "quit;\n";
  close ORA;
  &log("��SQL�������Ͽ����ݿ�����");
}

##################################################
#   ������get_sql_result
#   ���ܣ�ִ�����ݿ�����ĺ���
##################################################
sub get_sql_result
{
	&log("��SQL��������ʼ���ݿ����ӣ����Ӵ�����$CONN_STR��");
  my $sql = shift;
  &log("��SQL������SQL��䣺��$sql��");
  my $sql_result = `sqlplus -s "$CONN_STR" <<!
  set feedback off
  set heading off
  set echo off
  set pagesize 0
  set serveroutput on
  set colsep ";"
  set trimspool on
  set trimout on
  $sql;
  commit;
  quit
  !
  `;
  &log("��SQL�������Ͽ����ݿ�����");
  chomp($sql_result);
  $sql_result=~s/^\s+|\s+$//g;
  return ($sql_result);
}


##################################################
#   ������exec_cmd
#   ���ܣ�ִ�в���ϵͳ������
##################################################
sub exec_cmd
{
  foreach (@_)
  {
    &log("��CMD����������ϵͳ�����$_��");
    if (system("$_") != 0)
    {
      &log("��CMD����������ִ��ʧ��");
      &log("####################�쳣�˳�recal.pl�ű�###################");
      exit 1;
    }
  }
}

##################################################
#   ������kill_shm
#   ���ܣ�kil��ָ���shm
##################################################
sub kill_shm
{
	my $shm_thread = shift;
	my $thread_state = `ps -ef|grep "$USER_NAME"|grep "run_shm"|grep "$shm_thread"|grep -v "grep"`;
	chomp(my $proc_id = `echo '$thread_state'|awk '{print \$2}'`);
	if($proc_id ne ""){
		&log("�̺߳�Ϊ��$shm_thread�Ĺ����ڴ潫��Kill��");
		exec_cmd("kill -INT $proc_id");
	}
}

##################################################
#   ������is_running
#   ���ܣ��ж�ָ���߳��Ƿ������У�������з���1�����򷵻�0
#   �������̺߳�
##################################################
sub is_running
{
	my $thread = shift;
	if(not defined $thread){
		&log("����is_running���ô���û���ṩ������\n");
		exit 1;
	}
	my $thread_num = `ps -ef|grep "$USER_NAME"|grep "run_"|grep "$thread"|grep -v "grep"|wc -l`;
	chomp($thread_num);
	$thread_num =~ s/^\s+|\s+$//g;
	return $thread_num;
}

##################################################
#   ������table_is_clear
#   ���ܣ��жϱ������ͼ���Ƿ�Ϊ�գ����Ϊ�շ����棬���򷵻ؼ�
#   ����������
##################################################
sub table_is_clear
{
	my $table_name = shift;
	if(not defined $table_name){
		&log("����table_is_clear���ô���û���ṩ������\n");
		exit 1;
	}
	$count = &get_sql_result("select count(*) from $table_name where rownum < 2");
	if(not defined $count){
		&log("����table_is_clear�����޷���ñ�[$table_name]�Ƿ�Ϊ��");
		exit 1;
	}
	return $count == 0;
}

##################################################
#   ������redo_fts
#   ���ܣ����²ɼ��ӿ�������ں���
##################################################
sub redo_fts
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("fts");
    print "ѡ��\n";
    $choice = <STDIN>;
    chomp($choice);
    
    if($choice eq "r"){
    	return;
    } elsif($choice eq "q"){
    	&log("####################�����˳�recal.pl�ű�###################");
    	exit 0;
    }
    elsif(rindex($choice."\$", "\$") == 13){
    	my $aThread_id = substr($choice, 0, 8);
    	my $aFile_type = substr($choice, 9, 4);
    	
    	my $TUI_PROCESS = substr($aThread_id,2,2);
    	my $TUI_PROC_THREAD = substr($aThread_id,4,4);
    	if($TUI_PROCESS eq "40" and $TUI_PROC_THREAD gt "0250"){
    		&redo_cbss_fts_single_filetype($aThread_id, $aFile_type);
    	}else{
    		&redo_fts_single_filetype($aThread_id, $aFile_type);
    	}
    }
    else{
    	print "��Ч��������ѡ��\n";
    }
  }
  print "\n";
}

##################################################
#   ������redo_fts_single_filetype
#   ���ܣ��������²ɼ��ӿ������̵߳ĺ���
#   ������thread_id �̺߳� file_type �ļ�����
##################################################
sub redo_fts_single_filetype
{
  my ($thread_id, $current_file_type) = @_;
  my $choice = 0;
  
  my $thread_id_8_width = substr($thread_id, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("��� $current_file_type �ɼ���־��ʼ ......");
      my @prov_sql_string = (
      "delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      #"update $THREAD_INFO{$thread_id}{'log_table'} set src_file = concat(src_file,'\@1') where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and src_file like \'%.gz\';",
      "update $THREAD_INFO{$thread_id}{'log_table'} set src_file = src_file||\'\@\'||(select max(nvl(substr(src_file, 29), 0))+1 from $THREAD_INFO{$thread_id}{'log_table'} " . 
        "           where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and file_id = \'$FILE_TYPE_INFO{$current_file_type}{'fid'}\' and src_file like \'_______$ACCT_CYCLE\%\')" .
        "  where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and file_id = \'$FILE_TYPE_INFO{$current_file_type}{'fid'}\' and src_file like \'_______$ACCT_CYCLE\%\.gz\';",
      "delete from fts_break_point where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and src_id = \'$current_file_type\';"
      );
      &exec_sql(@prov_sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
    &log("��� $current_file_type ����Ŀ¼��ʼ .........");
    &exec_cmd("cd ${UNICAL_DT}/bps/$current_file_type/in;ls|egrep \"^\.\*0$current_file_type\\\.A0.gz\$\"|xargs -i rm -rf {}");
	if (substr($thread_id, 4, 2) eq "02") {
		&ssh_rm_remote($thread_id, $current_file_type);
	}
    &log("��� $current_file_type ����Ŀ¼���� .........");

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  &log("�����ɼ��̣߳�$FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_fts -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'fts_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'} > /dev/null 2>&1 &");
}
##################################################
#   ������redo_cbss_fts_single_filetype
#   ���ܣ��������²ɼ��ӿ������̵߳ĺ���
#   ������thread_id �̺߳� file_type �ļ�����
##################################################
sub redo_cbss_fts_single_filetype
{
  my ($thread_id, $current_file_type) = @_;
  my $choice = 0;
  
  my $thread_id_8_width = substr($thread_id, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("��� $current_file_type �ɼ���־��ʼ ......");
      my @prov_sql_string = (
      "delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      #"update $THREAD_INFO{$thread_id}{'log_table'} set src_file = concat(src_file,'\@1') where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and src_file like \'%.gz\';",
      "update $THREAD_INFO{$thread_id}{'log_table'} set src_file = src_file||\'\@\'||(select max(nvl(substr(src_file, 29), 0))+1 from $THREAD_INFO{$thread_id}{'log_table'} " . 
        "           where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and file_id = \'$FILE_TYPE_INFO{$current_file_type}{'fid'}\' and src_file like \'_______$ACCT_CYCLE\%\')" .
        "  where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\' and file_id = \'$FILE_TYPE_INFO{$current_file_type}{'fid'}\' and src_file like \'_______$ACCT_CYCLE\%\.gz\';",
      "delete from fts_break_point where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and src_id = \'$current_file_type\';"
      );
      &exec_sql(@prov_sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
    &log("��� $current_file_type ����Ŀ¼��ʼ .........");
    &exec_cmd("cd ${UNICAL_DT}/bps/$current_file_type/in;ls|egrep \"^\.\*1$current_file_type\\\.A1\$\"|xargs -i rm -rf {}");
	if (substr($thread_id, 4, 2) eq "02") {
		&ssh_rm_remote($thread_id, $current_file_type);
	}
    &log("��� $current_file_type ����Ŀ¼���� .........");

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  &log("�����ɼ��̣߳�$thread_id ��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_fts -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'fts_proc_id'} -t $thread_id eoc_tid$thread_id > /dev/null 2>&1 &");
}

##################################################
#   ������redo_check
#   ���ܣ����»��˽ӿ�������ں���
##################################################
sub redo_check
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("bps");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice >= 1 and $choice <= 21){&redo_check_single_filetype($FILE_TYPES[$choice-1]);}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
#   ������redo_check_single_filetype
#   ���ܣ��������»��˽ӿ������̵߳ĺ���
#   �������: $current_file_type �ļ�����
##################################################
sub redo_check_single_filetype
{
  (my $current_file_type) = @_;
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  my $thread_id_8_width = substr($FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_file_type] ������־��ʼ......");
      if($HOST_NAME eq "bdsdb1" or $HOST_NAME eq "bdsdb2"){
      	@sql_string = (
      	"delete from LOG_THREAD_STATE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      	"delete from LOG_BPS where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'$current_file_type\';",
      	"delete from LOG_BPS_LIST where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'$current_file_type\';"
      	);
    	}else{
    		@sql_string = (
      	"delete from LOG_THREAD_STATE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      	"insert into LOG_BPS_HIS select a.*, sysdate from LOG_BPS a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.file_type = \'$current_file_type\'; ",
      	"delete from LOG_BPS where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'$current_file_type\';",
      	"insert into LOG_BPS_LIST_HIS select a.*, sysdate from LOG_BPS_LIST a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.file_type = \'$current_file_type\';",
      	"delete from LOG_BPS_LIST where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'$current_file_type\';"
      	);
    	}
      &exec_sql(@sql_string);
      &trunc_table("err_bps_$current_file_type");
      &trunc_table("dup_bps_$current_file_type");
      
      if(table_is_clear("dup_bps_$current_file_type")){
      	&log("��������������[DUP_BPS_$current_file_type]�Ѿ������");
      }else{
      	&log("��������������[DUP_BPS_$current_file_type]δ����ճɹ��������Ժ��ٲ�����������DBA�鿴���Ƿ�����");
      	exit 1;
      }
      
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_file_type] ���ɵĹ����ڴ��ļ���ʼ ......");
      if(not -e "$UNICAL_DT/shm/public/$ACCT_CYCLE/dat"){
        print "Ŀ¼������,�봴��.[$UNICAL_DT/shm/public/$ACCT_CYCLE/dat].����δ�ܳɹ�ִ��.\n";
        exit 3;
      }
        
      if($current_file_type eq "CUST"){
        &exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_CUST_INFO\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "ACCT"){
        &exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_ACCT_INFO\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "USER"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_IDX_USER_INFO\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "UEXT"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_USER_INFO_DETAIL\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "TERM"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_IDX_USER_TERMINAL\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "PROJ"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_IDX_USER_PROJECT\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "PROM"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_IDX_USER_MARKETING\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_USER_PROMOTION\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "ORDR"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_IDX_USER_PRODUCT\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_PROD_ORDER\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_PROD_ORDER_R\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "APAY"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_USER_PRE_DEPOSIT\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_ADVANCED_PAY\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "RECE"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_RECEIVABLES\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_ACCT_ACCU\\\.seq\.\*RECE\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "PAID"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_PAID_UP\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_ACCT_ACCU\\\.seq\.\*PAID\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "USAG"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_SVC_USED\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "ARRE"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_USER_ARREAR\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "CARD"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_CARD_SALE\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "PAYO"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_PAY_OFF\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "PFFA"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_PAYFEE_AGENT\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_file_type eq "SERV"){
      	&exec_cmd("cd $UNICAL_DT/shm/public/$ACCT_CYCLE/dat;ls |egrep \"^AI_SERVICE\\\.seq\.\*\$\"|xargs -i rm -rf {}");
      }else{
      	&log("$current_file_type�������ɹ����ڴ��ļ�����������");
      }
      
      if($FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'} ne ""){
        &log("��� [$current_file_type] ʡ�ݻ�����������ļ���ʼ ......");
        if(-e "$UNICAL_DT/stat/P$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/P$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[$UNICAL_DT/stat/P$current_file_type/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }
      
      if($current_file_type eq "NSDF"){
      	if(-e "$UNICAL_DT/stat/P$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/P$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[$UNICAL_DT/stat/P$current_file_type/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
        
        if(-e "$UNICAL_DT/stat/$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[$UNICAL_DT/stat/$current_file_type/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }
      
      &log("��� [$current_file_type] ���ɵ�����Դ�ļ���ʼ ......");
      if($current_file_type eq "SERV"){
        if(-e "${UNICAL_DT}/unical/CRBO/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRBO/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRBO/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      
      }elsif($current_file_type eq "CARD"){
        if(-e "${UNICAL_DT}/unical/CRCS/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRCS/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRCS/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PAYO"){
        if(-e "${UNICAL_DT}/unical/CRCI/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRCI/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRCI/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "USER"){
        if(-e "${UNICAL_DT}/unical/CRUP/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRUP/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRUP/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PROM"){
        if(-e "${UNICAL_DT}/unical/CRPM/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPM/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPM/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "ORDR"){
        if(-e "${UNICAL_DT}/unical/CRPO/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPO/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPO/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "APAY"){
        if(-e "${UNICAL_DT}/unical/CRYC/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRYC/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRYC/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PFFA"){
        if(-e "${UNICAL_DT}/unical/CRPD/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage1/in;ls|egrep \"^\.\*PFFA\.\*\$\"|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPD/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "BUSI" ){
        if(-e "${UNICAL_DT}/unical/CRPD/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage1/in;ls|egrep \"^\.\*BUSI\.\*\$\"|xargs -i rm -rf {}");
        }else{
          print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPD/stage1/in].����δ�ܳɹ�ִ��.\n";
          exit 3;        	
        }
      }else{
      	&log("[$current_file_type] ������������Դ�ļ�����������");
      }
      
      &log("��� [$current_file_type] ���Ŀ¼ [inv, dup, err, agg] ��ʼ......");
      if(not -e "$UNICAL_DT/bps/$current_file_type/agg" or 
         not -e "$UNICAL_DT/bps/$current_file_type/dup" or
         not -e "$UNICAL_DT/bps/$current_file_type/err" or
         not -e "$UNICAL_DT/bps/$current_file_type/inv")
      {
        print "Ŀ¼������,�봴��.[$UNICAL_DT/bps/$current_file_type/agg][$UNICAL_DT/bps/$current_file_type/dup][$UNICAL_DT/bps/$current_file_type/err][$UNICAL_DT/bps/$current_file_type/inv].����δ�ܳɹ�ִ��.\n";
        exit 3;        	
      }
      
      @cmd_string = (
      "cd $UNICAL_DT/bps/$current_file_type/agg; ls|xargs -i rm -rf {}",
      "cd $UNICAL_DT/bps/$current_file_type/dup; ls|xargs -i rm -rf {}",
      "cd $UNICAL_DT/bps/$current_file_type/err; ls|xargs -i rm -rf {}",
      "cd $UNICAL_DT/bps/$current_file_type/inv; ls|xargs -i rm -rf {}"
      );
      &exec_cmd(@cmd_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'};
  }

  &log("���������ڴ��̣߳�$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'shm_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'} > /dev/null 2>&1 &");
  
  print "��ʼsleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}_$COMM_CYCLE.log��־��\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}�����������\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;


  &log("���������̣߳�$FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_bps -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'bps_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������redo_unical
#   ���ܣ����¼���ָ������Դ��ں���
##################################################
sub redo_unical
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("unical");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&redo_unical_single_driver_stage1($DRIVER_TYPES[$choice-1]);}
    elsif($choice eq "10"){&redo_unical_single_driver_stage2;}
    elsif($choice >= 11 and $choice <= 19){&redo_unical_single_driver_stage3($DRIVER_TYPES[$choice-11]);}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
#   ������redo_unical_single_driver_stage1
#   ���ܣ��������¼����һ�׶��̵߳ĺ���
##################################################
sub redo_unical_single_driver_stage1
{
  (my $current_driver_type) = @_;
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  my $sql_id;
  my $msg ;
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_driver_type] ����Դ��һ�׶ε���־��ʼ ......");
      #if($HOST_NAME eq "bdsdb1" or $HOST_NAME eq "bdsdb2"){
    	#  @sql_string = (
    	#  "delete from LOG_THREAD_STATE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
    	#  "delete from LOG_UNIPRE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
    	#  "delete from LOG_UNIPRE_LIST where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
    	#  "delete from err_unipre_$current_driver_type where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'}\';"
    	#  );
    	#}else{
    		@sql_string = (
    	  "delete from LOG_THREAD_STATE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
    	  "insert into LOG_UNIPRE_HIS select a.*, sysdate from LOG_UNIPRE a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.FILE_TYPE = \'$current_driver_type\';",
    	  "delete from LOG_UNIPRE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
    	  "insert into LOG_UNIPRE_LIST_HIS select a.*, sysdate from LOG_UNIPRE_LIST a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.FILE_TYPE = \'$current_driver_type\';",
    	  "delete from LOG_UNIPRE_LIST where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
    	  "delete from err_unipre_$current_driver_type where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'}\';"
    	  );
    	#}
      &exec_sql(@sql_string);
      
      #����˻�����ϸ�Ĳ��ر�
      $sql_id = "$UNICAL_PROVINCE_CODE" ."_del_UNIDUP";
      $msg = `cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n��������[DUP_UNIPRE]�����ݼ�¼�������Ѿ�ִ��.\n";

      $sql_id = "$UNICAL_PROVINCE_CODE" ."_sel_UNIDUP";
      $msg = `cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n�����[DUP_UNIPRE]�����ݼ�¼������[$msg].\n";
      if($msg eq "0") {
        print "��������[DUP_UNIPRE] �ɹ�.\n";
      }else{
        print "��������[DUP_UNIPRE] ʧ��.\n";
      }
      
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_driver_type] ��һ�׶����Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/dup" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/err" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/inv" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/in"  or
         not -e "${UNICAL_DT}/busi/in")
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/$current_driver_type/stage1/dup][${UNICAL_DT}/unical/$current_driver_type/stage1/err][${UNICAL_DT}/unical/$current_driver_type/stage1/inv][${UNICAL_DT}/unical/$current_driver_type/stage3/in][${UNICAL_DT}/busi/in].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      @cmd_string=(
      "cd ${UNICAL_DT}/unical/$current_driver_type/stage1/err; ls|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/unical/$current_driver_type/stage1/inv; ls|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/unical/$current_driver_type/stage3/in;  ls|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/busi/in; ls |egrep \"^\.\*$current_driver_type\.\*\$\"|xargs -i rm -rf {}"
      );
      &exec_cmd(@cmd_string);
	@cmd_mv=(
      	"cd ${UNICAL_DT}/unical/$current_driver_type/stage1/bak; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage1/in",
      	"cd ${UNICAL_DT}/unical/$current_driver_type/stage1/dup; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage1/in",
      	"cd ${UNICAL_DT}/unical/$current_driver_type/stage1/tmp; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage1/in"
	);
      &exec_cmd(@cmd_mv);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'};
  }

  &log("���������ڴ��̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'} > /dev/null 2>&1 &");

  print "��ʼsleep 3 Seconds...\n";
  sleep(3);
  print "$HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}_$COMM_CYCLE.log��־��\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}�����������\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("����Ӷ������һ�׶��̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������redo_unical_single_driver_stage2
#   ���ܣ��������¼���ڶ��׶��̵߳ĺ���
##################################################
sub redo_unical_single_driver_stage2
{
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  
  if(is_running("19200101")){
  	&log("[19200101]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("���Ӷ�����ڶ�������־��ʼ ......");
      if($HOST_NAME eq "bdsdb1" or $HOST_NAME eq "bdsdb2"){
      	@sql_string = (
      	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';",
      	"delete from log_unipre where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';",
      	"delete from log_unipre_list where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'CRUP\' and stage = 2;",
      	"delete from err_unipre_crup where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';"
      	);
      }else{
      	@sql_string = (
      	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';",
      	"insert into log_unipre_his select a.*, sysdate from log_unipre a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.thread_id = \'19200101\';",
      	"delete from log_unipre where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';",
      	"insert into log_unipre_list_his select a.*, sysdate from log_unipre_list a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.file_type = \'CRUP\' and a.stage = 2;",
      	"delete from log_unipre_list where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'CRUP\' and stage = 2;",
      	"delete from err_unipre_crup where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'19200101\';"
      	);
			}
      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��յڶ��׶����Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      if(not -e "${UNICAL_DT}/busi/his" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/busi/his].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }      
      @cmd_string=(
      "cd ${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat; ls |egrep \"^ai_idx_busi_value\\\.seq\.\*\$\"|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat; ls |egrep \"^ai_idx_acct_busi_value\\\.seq\.\*\$\"|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/busi/his; ls |egrep \"^[0-9]{10}${ACCT_CYCLE}CRUP\\\.G[01]\$\"|xargs -i rm -rf {}"
      ); 
      &exec_cmd(@cmd_string);
      
      @cmd_mv=(
      "cd ${UNICAL_DT}/busi/bak; ls |egrep \"^[0-9]{10}${ACCT_CYCLE}*\\\.F[01]\$\"|xargs -i mv {} ${UNICAL_DT}/busi/in"
      ); 
      &exec_cmd(@cmd_mv);

      last;          
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne "01300101";
  }

  &log("���������ڴ��̣߳�01300101��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p 30 -t 01300101 eoc_tid01300101 > /dev/null 2>&1 &");

  print "��ʼsleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/01300101_$COMM_CYCLE.log��־��\n";
  my $pid = open HD,"tail -f $HOME/log/shm/01300101_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "01300101�����������\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("����Ӷ�����ڶ��׶��̣߳�19200101��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p 20 -t 19200101 eoc_tid19200101 > /dev/null 2>&1 &");
}

##################################################
#   ������redo_unical_single_driver_stage3
#   ���ܣ��������¼�������׶��̵߳ĺ���
##################################################
sub redo_unical_single_driver_stage3
{
  (my $current_driver_type) = @_;
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("���$current_driver_type����Դ�����׶ε���־��ʼ......");
      if($HOST_NAME eq "bdsdb1" or $HOST_NAME eq "bdsdb2"){
      	@sql_string = (
      	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      	"delete from log_unical where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
      	"delete from log_unical_list where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
      	"delete from err_unical_$current_driver_type where SETT_PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'}\';"
      	);
      }else{
      	@sql_string = (
      	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
      	"insert into log_unical_his select a.*, sysdate from log_unical a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.FILE_TYPE = \'$current_driver_type\';",
      	"delete from log_unical where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
      	"insert into log_unical_list_his select a.*, sysdate from log_unical_list a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.FILE_TYPE = \'$current_driver_type\';",
      	"delete from log_unical_list where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and FILE_TYPE = \'$current_driver_type\';",
      	"delete from err_unical_$current_driver_type where SETT_PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'}\';"
      	);
			}
      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_driver_type] �����׶����Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/dup" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/err" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/inv" or
         not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in")
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/$current_driver_type/stage3/dup][${UNICAL_DT}/unical/$current_driver_type/stage3/err]".
              "[${UNICAL_DT}/unical/$current_driver_type/stage3/inv][${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      @cmd_string = (
      "cd ${UNICAL_DT}/unical/$current_driver_type/stage3/err; ls|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/unical/$current_driver_type/stage3/inv;  ls|xargs -i rm -rf {}",
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in; ls|xargs -i rm -rf {}"
      );
      &exec_cmd(@cmd_string);
	@cmd_mv =(
	"cd ${UNICAL_DT}/unical/$current_driver_type/stage3/bak; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage3/in",
	"cd ${UNICAL_DT}/unical/$current_driver_type/stage3/dup; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage3/in",
	"cd ${UNICAL_DT}/unical/$current_driver_type/stage3/tmp; ls|xargs -i mv {} ${UNICAL_DT}/unical/$current_driver_type/stage3/in"
	);
	&exec_cmd(@cmd_mv);
      
      &log("��� [$current_driver_type] ���ɵĶ����ļ���ʼ ......");
      if(not -e "${UNICAL_DT}/stat/SSFD/in" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/SSFD/in].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      if($current_driver_type eq "CRBO"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*SERV\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRCS"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*CARD\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRCI"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*PAYO\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRUP"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*USER\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRPM"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*PROM\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRPO"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*ORDR\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRYC"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*APAY\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRPD"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*PFFA\.\*\$\"|xargs -i rm -rf {}");
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*BUSI\.\*\$\"|xargs -i rm -rf {}");
      }elsif($current_driver_type eq "CRCH"){
      	&exec_cmd("cd ${UNICAL_DT}/stat/SSFD/in; ls |egrep \"^\.\*CHNL\.\*\$\"|xargs -i rm -rf {}");
      }
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'};
  }

  &log("���������ڴ��̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'} > /dev/null 2>&1 &");

  print "��ʼsleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}_$COMM_CYCLE.log��־��\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}�����������\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("����Ӷ���������׶��̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unical_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������redo_stat
#   ���ܣ�����������ں���
##################################################
sub redo_stat
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("sub_stat");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice eq "1"){&redo_stat_prov;}
    elsif($choice eq "2"){&redo_stat_comm;}
    elsif($choice eq "3"){&redo_stat_frz;}
    elsif($choice eq "4"){&redo_stat_sett;}
    elsif($choice eq "5"){&redo_stat_comp;}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
#   ������redo_stat_prov
#   ���ܣ�ʡ��ԭʼ��������������ں���
##################################################
sub redo_stat_prov
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_prov");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice >= 1 and $choice <= 20){&stat_prov($FILE_TYPES[$choice-1]);}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
# ������stat_prov
# ���ܣ�ʡ��ԭʼ������������ʵ�ֺ���
# ������ thread_id �̺߳� current_file_type �ӿ��ļ�����
##################################################
sub stat_prov
{
  my ($thread_id, $current_file_type) = @_;
  my $current_stat_key;
  if($current_file_type eq "NSDF"){
    $current_stat_key = "$current_file_type";
  }elsif($current_file_type eq "NPRE"){
    $current_stat_key = "$current_file_type";
  }else{
    $current_stat_key = "P$current_file_type";
  }

  
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;

  #��ȡʵ�ʵ�Ӷ������$actrual_cycle������������������������ڣ������������е�����$acct_cycle���бȽϣ������һ�£���˵��Ҫͬ����ʷ���ڵ����ݣ�������
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "����ϵͳʱ�����㵱ǰ������[$actrual_cycle], �������������õ�������[$ACCT_CYCLE], ����ִ����������\n";
    print "��������ǰѵ�ǰ���ڵĻ������ݽ�����������������ֻ�ܲ�����ǰ���ڵ����ݡ�\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "���ʡ��ԭʼ��������������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] �����־��ʼ......");
     	@sql_string = (
     	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_MON a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.THREAD_ID = \'$thread_id\';",
     	"delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\';",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_DAY a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.THREAD_ID = \'$thread_id\';",
     	"delete from LOG_STAT_DAY where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'$thread_id\';"
     	);

      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "��Ч��������ѡ��\n";
    }
  }
  
  if($current_stat_key ne "NSDF" and $current_stat_key ne "NPRE"){
    while(1){
    	if(!$TUI_THREAD_FLAG){
        print "������ݱ� [$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE]��y|Y���,n|N���������\n";
        $choice=<STDIN>;
        chomp($choice);
      }else{$choice = "y";}   
    
      if($choice eq "y" or $choice eq "Y"){
        &log("��� [$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE] ������ݱ�ʼ ......");
        &trunc_table("$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE");
        last;
      }elsif($choice eq "n" or $choice eq "N"){
        last;
      }else{
      	print "��Ч��������ѡ��\n";
      }
    }
  }
  
  if($current_stat_key eq "PPFFA"){
    while(1){
    	if(!$TUI_THREAD_FLAG){
        print "������ݱ� [PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE]��y|Y���,n|N���������\n";
        $choice=<STDIN>;
        chomp($choice);
      }else{$choice = "y";}   
    
      if($choice eq "y" or $choice eq "Y"){
        &log("��� [PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE] ������ݱ�ʼ ......");
        &trunc_table("PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE");
        last;
      }elsif($choice eq "n" or $choice eq "N"){
        last;
      }else{
      	print "��Ч��������ѡ��\n";
      }
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] ���Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/$current_stat_key/dup].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      #@cmd_string = (
      #"cd ${UNICAL_DT}/stat/$current_stat_key/dup; ls|xargs -i rm -rf {}"
      #);
      #&exec_cmd(@cmd_string);
      if ($thread_id eq "14200101"){
      @cmd_mv =(
      "cd ${UNICAL_DT}/stat/NPRE/in/; ls|xargs -i mv {} ${UNICAL_DT}/stat/NSDF/in",
      "cd ${UNICAL_DT}/stat/NSDF/dup/; ls|xargs -i mv {} ${UNICAL_DT}/stat/NSDF/in"
      );
      }else{
      @cmd_mv =(
      "cd ${UNICAL_DT}/stat/$current_stat_key/bak/; ls|xargs -i mv {} ${UNICAL_DT}/stat/$current_stat_key/in",
      "cd ${UNICAL_DT}/stat/$current_stat_key/dup/; ls|xargs -i mv {} ${UNICAL_DT}/stat/$current_stat_key/in"
      );
      }
    &exec_cmd(@cmd_mv);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
       last;
    }else{
       print "��Ч��������ѡ��\n";
    }
  }
  
  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;

  &log("��������̣߳�$FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'prov_stat_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������redo_stat_comm
#   ���ܣ�Ӷ�������ϸ���»��ܵ���ں���
##################################################
sub redo_stat_comm
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_comm");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&stat_comm($DRIVER_TYPES[$choice-1]);}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
#   ������stat_comm
#   ���ܣ�Ӷ�������ϸ���»��ܵ�ʵ�ֺ���
##################################################
sub stat_comm
{
  my ($current_driver_type) = @_;
  my $current_stat_key = "COMM";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;

  #��ȡʵ�ʵ�Ӷ������$actrual_cycle������������������������ڣ������������е�����$acct_cycle���бȽϣ������һ�£���˵��Ҫͬ����ʷ���ڵ����ݣ�������
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "����ϵͳʱ�����㵱ǰ������[$actrual_cycle], �������������õ�������[$ACCT_CYCLE], ����ִ��Ӷ����ϸ��������\n";
    print "��������ǰѵ�ǰ���ڵ�Ӷ����ϸ���ݽ�����������������ֻ�ܲ�����ǰ���ڵ����ݡ�\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "���Ӷ�������ϸ�������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] �����־��ʼ ......");
     	@sql_string = (
     	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_MON a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}',1,8);",
     	"delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}',1,8);",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_DAY a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}',1,8);",
     	"delete from LOG_STAT_DAY where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}',1,8);"
     	);
      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] ���Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      @cmd_string = (
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup; ls|xargs -i rm -rf {}"
      );
      &exec_cmd(@cmd_string);
      @cmd_mv= (
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup; ls|xargs -i mv {} ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in",
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/bak; ls|xargs -i mv {} ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in"
      );
      &exec_cmd(@cmd_string);
      &exec_cmd(@cmd_mv);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;

  &log("��������̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������redo_stat_frz
#   ���ܣ�������ϸ����������ں���
##################################################
sub redo_stat_frz
{
  &stat_frz;
  print "\n";
}

##################################################
#   ������stat_frz
#   ���ܣ�������ϸ��������ʵ�ֺ���
##################################################
sub stat_frz
{
  my $current_stat_key = "SSFD";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  
  #��ȡʵ�ʵ�Ӷ������$actrual_cycle������������������������ڣ������������е�����$acct_cycle���бȽϣ������һ�£���˵��Ҫͬ����ʷ���ڵ����ݣ�������
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "����ϵͳʱ�����㵱ǰ������[$actrual_cycle], �������������õ�������[$ACCT_CYCLE], ����ִ�ж�����ϸ��������\n";
    print "��������ǰѵ�ǰ���ڵĶ�����ϸ���ݽ�����������������ֻ�ܲ�����ǰ���ڵ����ݡ�\n";
    exit 1;  
  }
  
  if(is_running("14600101")){
  	&log("[14600101]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] �����־��ʼ ......");
     	@sql_string=(
     	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'14600101\';",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_MON a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.THREAD_ID = \'14600101\';",
     	"delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and THREAD_ID = \'14600101\';"
     	);
      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("���$current_stat_key���Ŀ¼��ʼ......");
      if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/$current_stat_key/dup].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      #@cmd_string = (
      #"cd ${UNICAL_DT}/stat/$current_stat_key/dup; ls|xargs -i rm -rf {}"
      #);
      #&exec_cmd(@cmd_string);
      @cmd_mv= (
      "cd ${UNICAL_DT}/stat/$current_stat_key/bak; ls|xargs -i mv {} ${UNICAL_DT}/stat/$current_stat_key/in",
      "cd ${UNICAL_DT}/stat/$current_stat_key/dup; ls|xargs -i mv {} ${UNICAL_DT}/stat/$current_stat_key/in"
      );
      &exec_cmd(@cmd_mv);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;

  &log("��������̣߳�14600101��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p 60 -t 1460010110 eoc_tid14600101 > /dev/null 2>&1 &");
}

##################################################
#   ������redo_stat_comp
#   ���ܣ�����ָ������������ں���
##################################################
sub redo_stat_comp
{
  &stat_comp;
  print "\n";
}

##################################################
#   ������stat_comp
#   ���ܣ�����ָ����������ʵ�ֺ���
##################################################
sub stat_comp
{
  my $current_stat_key = "COMP";
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  #��ȡʵ�ʵ�Ӷ������$actrual_cycle������������������������ڣ������������е�����$acct_cycle���бȽϣ������һ�£���˵��Ҫͬ����ʷ���ڵ����ݣ�������
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "����ϵͳʱ�����㵱ǰ������[$actrual_cycle], �������������õ�������[$ACCT_CYCLE], ����ִ�и���ָ����������\n";
    print "��������ǰѵ�ǰ���ڵĸ���ָ�����ݽ�����������������ֻ�ܲ�����ǰ���ڵ����ݡ�\n";
    exit 1;  
  }
  
  if(is_running("14610101")){
  	&log("[14610101]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] �����־��ʼ ......");
      &log("�������ָ��������־���Լ����ݱ�......");
      `cd $RELEASE_BIN_PATH;exec_sql_encrypt -i delete_comp`;
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      ## &log("���$current_stat_key���Ŀ¼��ʼ......");
      ## if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      ## {
      ##   print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/$current_stat_key/dup].����δ�ܳɹ�ִ��.\n";
      ##   exit 3;      
      ## }
      ## @cmd_string = (
      ## "cd ${UNICAL_DT}/stat/$current_stat_key/dup; ls|xargs -i rm -rf {}"
      ## );
      ## &exec_cmd(@cmd_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;

  &log("��������̣߳�14610101��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p 60 -t 1461010110 eoc_tid14610101 > /dev/null 2>&1 &");
}

##################################################
#   ������redo_stat_sett
#   ���ܣ��������˵���ں���
##################################################
sub redo_stat_sett
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_sett");
    print "ѡ��\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################�����˳�recal.pl�ű�###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&stat_sett($DRIVER_TYPES[$choice-1]);}
    else{print "��Ч��������ѡ��\n";}
  }
  print "\n";
}

##################################################
#   ������stat_sett
#   ���ܣ��������˵�ʵ�ֺ���
##################################################
sub stat_sett
{
  (my $current_driver_type) = @_;
  my $current_stat_key = "SETT";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;

  #��ȡʵ�ʵ�Ӷ������$actrual_cycle������������������������ڣ������������е�����$acct_cycle���бȽϣ������һ�£���˵��Ҫͬ����ʷ���ڵ����ݣ�������
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "����ϵͳʱ�����㵱ǰ������[$actrual_cycle], �������������õ�������[$ACCT_CYCLE], ����ִ�����������\n";
    print "��������ǰѵ�ǰ���ڵ�Ӷ�����ݽ��������˲�������ֻ�ܲ�����ǰ���ڵ����ݡ�\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "������ݿ���־��y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){                     
      &log("��� [$current_stat_key] �����־��ʼ ......");
     	@sql_string = (
     	"delete from log_thread_state where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id like \'$thread_id_8_width\%\';",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_MON a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}',1,8);",
     	"delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}',1,8);",
     	"insert into LOG_STAT_HIS select a.*, sysdate from LOG_STAT_DAY a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}',1,8);",
     	"delete from LOG_STAT_DAY where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = substr('$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}',1,8);"
     	);

      &exec_sql(@sql_string);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "����������Ч��������ļ���y|Y���,n|N���������\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("��� [$current_stat_key] ���Ŀ¼��ʼ ......");
      if(not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2" )
      {
        print "Ŀ¼������,�봴��.[${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2].����δ�ܳɹ�ִ��.\n";
        exit 3;      
      }
      #@cmd_string = (
      #"cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2; ls|xargs -i rm -rf {}"
      #);
      #&exec_cmd(@cmd_string);
      @cmd_mv= (
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/bak2; ls|xargs -i mv {} ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/bak",
      "cd ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2; ls|xargs -i mv {} ${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/bak"
      );
      &exec_cmd(@cmd_mv);
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "��Ч��������ѡ��\n";
    }
  }

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
  #print "�����������, ׼����������߳�...\n";
  return if $TUI_THREAD_FLAG;

  &log("��������̣߳�$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}��ʼ......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   ������check_if_directory
#   ���ܣ�����Ƿ�ΪĿ¼
#   ���������������
##################################################
sub check_if_directory
{
  my $name = $_[0];
  if(opendir DH, $name){
     closedir DH;
     return 1;
  }
  return 0;  
}

##################################################
#   ������recursive_traversal_remove_file
#   ���ܣ��ݹ����һ��Ŀ¼������Ŀ¼�������е��ļ�ɾ������������Ŀ¼������ļ�Ҳ�ᱻɾ����
#   ������Ҫɾ���ļ��ĸ�Ŀ¼
##################################################
sub recursive_traversal_remove_file
{
	#��ȡ�ݹ��Ŀ¼
  my $root = $_[0]; 
  my $dir_handle = sprintf("DH%d", ++$globle_index);
  opendir $dir_handle, $root or die "Can't open directory,information:$!!\n";
  my @dirs = readdir $dir_handle;
  foreach(@dirs){
    #if(check_if_directory("$root/$_")){  #�����Ŀ¼
    if(-d "$root/$_"){  #�����Ŀ¼
    	if(not /^(\.|\.\.)$/){
        &log("��ʼ����Ŀ¼$root/$_");
        recursive_traversal_remove_file("$root/$_") if (not /^(\.|\.\.)$/);
      }
    }else{    #������ļ�
      &exec_cmd("rm -f $root/$_");
    }
  }
  
  closedir $dir_handle;
}

##################################################
#   ������get_object_name
#   ���ܣ������ݿ��л�ȡָ��ƥ��ģʽ�Ķ�������
#   ������ƥ��ģʽ
##################################################
sub get_object_name
{
	my $pattern = shift;
	my $object_names = &get_sql_result("select object_name from user_objects where object_name like '$pattern'");
	my @object_names = split(/\n/, $object_names);
	return @object_names;
}

##################################################
#   ������clear_all
#   ���ܣ�����Ӷ������������Ŀ¼�ļ�
#   ��������
##################################################
sub clear_all
{
	my $choice = 0;
	if(!$TUI_THREAD_FLAG){
	  print "�ò�������ɾ����������[$UNICAL_DT]���������ļ�\n";
	  print "�ò����������[$CONN_STR]���������ݲɼ������ˣ����㣬������л��ڵ���־��\n";
	  print "�����ʹ�ã���\n";
	  print "ȷ��Ҫ���иò�����������YESȷ����\n";
	  $choice = <STDIN>;
	  chomp($choice);
	}else{$choice = "YES"};
		
	if($choice eq "YES"){
	  print "�ò�������ɾ����������[$UNICAL_DT]���������ļ�\n";
		print "please wait ...\n";
		&log("��ʼ�ݹ�ɾ�� $UNICAL_DT Ŀ¼�µ������ļ� ......");
    &recursive_traversal_remove_file("$UNICAL_DT");
    
    &log("��ʼ�ݹ�ɾ�� $HOME/log Ŀ¼�µ������ļ� ......");
    &recursive_traversal_remove_file("$HOME/log");
    
    &log("��ʼ�ݹ�ɾ�� $HOME/debug/log Ŀ¼�µ������ļ� ......");
    &recursive_traversal_remove_file("$HOME/debug/log");
    
    #&clear_table_truncate();  #��Ҫʹ�� truncate ����ʱ��ʹ���������
    &clear_table_province();  #��Ҫ��ʡ��ձ��ʱ��ʹ������������������ʹ�� delete ���
    
    
  }else{
  	print "����ȡ��!\n";
  	return;
  }
}
##################################################
#   ������ clear_table_province
#   ���ܣ� ʹ�� delete ��䰴ʡ�ѱ����
#   ������ ��
##################################################
sub clear_table_province
{
	my @sql_string;
	
  &log("��ʼ��ʡ��� exp ����־�� ...");
  @sql_string = (
  "delete from LOG_EXP where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from EXP_BREAK_POINT where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);
	
  &log("��ʼ��ʡ��� fts ����־�� ...");
  @sql_string = (
  "delete from LOG_FTS_PROV where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_FTS_NS   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_FTS_RR   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from FTS_BREAK_POINT where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);

  # BPS �������־������
  &log("��ʼ��ʡ��� bps ����־�� ...");
  @sql_string = (
  "delete from LOG_BPS where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_BPS_LIST   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);

  &log("��ʼ��ʡ��� bps �Ĳ��ع����� ...");
  @object_names = &get_object_name("DUP_BPS\%");
  &trunc_table(@object_names);
 
  @object_names = &get_object_name("ERR_BPS\%");
  &trunc_table(@object_names);
    
  # UNICAL �������־������
  &log("��ʼ��� unical ����־��...");
  @sql_string = (
  "delete from LOG_UNIPRE        where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_UNIPRE_LIST   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_UNICAL        where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_UNICAL_LIST   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);
  
  @object_names = &get_object_name("DUP_UNI");
  &trunc_table(@object_names);
  
  @object_names = &get_object_name("ERR_UNI\%");
  &trunc_table(@object_names);
      
    
  # STAT �������־������
  &log("��ʼ��� stat ����־��...");
  @sql_string = (
  "delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_STAT_DAY where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);
    
    &log("��ʼ�����������־��...");
    @object_names = ();
    push(@object_names,"LOG_THREAD_STATE");
    #push(@object_names,"LOG_UNICAL_STAT");
    #push(@object_names,"ERROR_REPORT");
    #push(@object_names,"LOG_PROVFEE_STAT");
    #push(@object_names,"LOG_PROVFILE_MON");
    &trunc_table(@object_names);
}

##################################################
#   ������ clear_table_truncate
#   ���ܣ� ʹ�� truncate ���ѱ����
#   ������ ��
##################################################
sub clear_table_truncate
{
    my @object_names;
    
    &log("��ʼ���exp����־��...");
    @object_names = ();
    push(@object_names, "LOG_EXP");
    push(@object_names, "EXP_BREAK_POINT");
    &trunc_table(@object_names);
    
    &log("��ʼ���fts����־��...");
    @object_names = &get_object_name("LOG_FTS\%");
    push(@object_names,"FTS_BREAK_POINT");
    &trunc_table(@object_names);
    
    &log("��ʼ���bps����־��...");
    @object_names = &get_object_name("LOG_BPS\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("ERR_BPS\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("DUP_BPS\%");
    &trunc_table(@object_names);
    
    &log("��ʼ���unical����־��...");
    @object_names = &get_object_name("LOG_UNI\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("ERR_UNI\%");
    &trunc_table(@object_names);
    
    &log("��ʼ���stat����־��...");
    @object_names = &get_object_name("LOG_STAT\%");
    &trunc_table(@object_names);
    
    &log("��ʼ�����������־��...");
    @object_names = ();
    push(@object_names,"LOG_THREAD_STATE");
    push(@object_names,"LOG_UNICAL_STAT");
    #push(@object_names,"ERROR_REPORT");
    #push(@object_names,"LOG_PROVFEE_STAT");
    #push(@object_names,"LOG_PROVFILE_MON");
    &trunc_table(@object_names);
	
}

##################################################
#   ������print_menu_list
#   ���ܣ���ӡ���ֲ˵����б�
##################################################
sub print_menu_list
{
  (my $flag) = (@_);
  my $file_type;
  my $index = 0;
  print "-"x50, "\n";
  
  if($flag eq "main"){
  	print " "x20,"���˵�\n";
    print "- 1  ���²ɼ�\n";
    print "- 2  ���»���\n";
    print "- 3  ���¼���\n";
    print "- 4  �������\n";
    print "- 5  �����������ã���\n";
  }
  elsif($flag eq "fts"){
  	print " "x20, "���²ɼ����˵�\n";
    print "11400102-USER\n";
    print "11400103-UEXT\n";
    print "11400113-TERM\n";
    print "11400114-PROJ\n";
    print "11400115-PROM\n";
    print "11400106-ORDR\n";
    print "11400104-APAY\n";
    print "11400116-PFFA\n";
    print "11400117-BUSI\n";
    print "11400118-SERV\n";
    print "11400119-CARD\n";
    print "11400105-RECE\n";
    print "11400120-PAID\n";
    print "11400121-ARRE\n";
    print "11400122-PAYO\n";
    print "11400123-USAG\n";
    print "11400124-GROU\n";
    print "11400125-BALA\n";
    print "11400101-CUST\n";
    print "11400126-ACCT\n";
    print "11200101-NSDF\n";
    print "11420101-T001\n";
    print "11420101-T002\n";
    print "11420101-T003\n";
    print "11420101-T004\n";
    print "11420101-T005\n";
    print "11420101-C001\n";
    print "11420101-C002\n";
    print "11420101-CBAL\n";
    print "11420101-DBAL\n";
    print "11300101-KPXS\n";
    print "11300102-USRD\n";
    print "11210101-NDAY\n";
    print "11500101-BDRR\n";
    print "11500102-COMD\n";
    print "11500103-BDR2\n";
    print "11400151-USER\n";
    print "11400152-UEXT\n";
    print "11400153-TERM\n";
    print "11400154-PROJ\n";
    print "11400155-PROM\n";
    print "11400156-ORDR\n";
    print "11400157-APAY\n";
    print "11400158-PFFA\n";
    print "11400159-BUSI\n";
    print "11400160-SERV\n";
    print "11400161-CARD\n";
    print "11400162-RECE\n";
    print "11400163-PAID\n";
    print "11400164-ARRE\n";
    print "11400165-PAYO\n";
    print "11400166-USAG\n";
    print "11400167-GROU\n";
    print "11400168-CUST\n";
    print "11400169-ACCT\n";
    print "11400170-BALA\n";
  }
  elsif($flag eq "bps"){
  	print " "x20,"���»������˵�\n";
    for $file_type(@FILE_TYPES)
    {
      printf "- %02d $file_type�ӿ�\n",++$index;
    }
  }
  elsif($flag eq "unical"){
  	print " "x20,"���¼������˵�\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type��һ�׶�\n",++$index;
    }
    printf "- %02d Ӷ�����ڶ��׶�\n",++$index;
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type�����׶�\n",++$index;
    }
  }
  elsif($flag eq "sub_stat"){
  	print " "x20,"����������˵�\n";
    print "- 1  ʡ��ԭʼ�����������\n";
    print "- 2  Ӷ�������ϸ���»���\n";
    print "- 3  ������ϸ�������\n";
    print "- 4  ��������\n";
    print "- 5  ����ָ���������\n";
  }
  elsif($flag eq "stat_prov"){
  	print " "x20,"ʡ��ԭʼ��������������˵�\n";
    for $file_type(@FILE_TYPES)
    {
      printf "- %02d P$file_type�ӿ�\n",++$index;
    }
  }
  elsif($flag eq "stat_comm"){
  	print " "x20,"Ӷ�������ϸ���»������˵�\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type���\n",++$index;
    }
  }
  elsif($flag eq "stat_sett"){
  	print " "x20,"�����������˵�\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type����\n",++$index;
    }
  }

  if($flag ne "main")
  {
    print "- r  �����ϲ�Ŀ¼\n";
  }
  print "- q  �˳�!\n";
  print "-"x50,"\n";
}

##################################################
#   ������ redo_13900101
#   ���ܣ� ��13900101�߳̽���������û���������
##################################################
sub redo_13900101
{
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  if(is_running("13900101")){
  	&log("[13900101]�߳��������У���ʱ����ִ��[REDO]����������ֹͣ�̣߳�Ȼ����ִ��[REDO]����");
  	exit 1;
  }
  
  &log("��� [FRZD] ������־��ʼ......");
	@sql_string = (
  	"delete from LOG_THREAD_STATE where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and thread_id = \'13900101\';",
   	"insert into LOG_BPS_HIS select a.*, sysdate from LOG_BPS a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.file_type = \'FRZD\'; ",
   	"delete from LOG_BPS where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'FRZD\';",
   	"insert into LOG_BPS_LIST_HIS select a.*, sysdate from LOG_BPS_LIST a where a.PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and a.file_type = \'FRZD\';",
   	"delete from LOG_BPS_LIST where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\' and file_type = \'FRZD\';"
   	);
  &exec_sql(@sql_string);
  &trunc_table("err_bps_FRZD");
      
  if(table_is_clear("err_bps_FRZD")){
  	&log("��������������[err_bps_FRZD]�Ѿ������");
   }else{
   	&log("��������������[err_bps_FRZD]δ����ճɹ��������Ժ��ٲ�����������DBA�鿴���Ƿ�����");
  	exit 1;
  }
      
  &log("��� [FRZD] ���ɵĽⶳ�ļ���ʼ ......");
  if(-e "${UNICAL_DT}/unical/CRBO/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRBO/stage3/in;ls|egrep \"^\.\*SERV\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRBO/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }
  
  if(-e "${UNICAL_DT}/unical/CRCS/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCS/stage3/in;ls|egrep \"^\.\*CARD\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRCS/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRCI/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCI/stage3/in;ls|egrep \"^\.\*PAYO\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRCI/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRUP/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRUP/stage3/in;ls|egrep \"^\.\*USER\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRUP/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPM/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPM/stage3/in;ls|egrep \"^\.\*PROM\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPM/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPO/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPO/stage3/in;ls|egrep \"^\.\*ORDR\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPO/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRYC/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRYC/stage3/in;ls|egrep \"^\.\*APAY\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRYC/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPD/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage3/in;ls|egrep \"^\.\*PFFA\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRPD/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }
  if(-e "${UNICAL_DT}/unical/CRCH/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCH/stage3/in;ls|egrep \"^\.\*CHNL\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "Ŀ¼������,�봴��.[${UNICAL_DT}/unical/CRCH/stage3/in].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }
      
  &log("��� [FRZD] ���Ŀ¼ [inv, dup, err, agg] ��ʼ......");
  if(not -e "$UNICAL_DT/bps/FRZD/agg" or 
     not -e "$UNICAL_DT/bps/FRZD/dup" or
     not -e "$UNICAL_DT/bps/FRZD/err" or
     not -e "$UNICAL_DT/bps/FRZD/inv")
  {
    print "Ŀ¼������,�봴��.[$UNICAL_DT/bps/FRZD/agg][$UNICAL_DT/bps/FRZD/dup][$UNICAL_DT/bps/FRZD/err][$UNICAL_DT/bps/FRZD/inv].����δ�ܳɹ�ִ��.\n";
    exit 3;        	
  }
  
  @cmd_string = (
  "cd $UNICAL_DT/bps/FRZD/agg; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/dup; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/err; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/inv; ls|xargs -i rm -rf {}"
  );
  &exec_cmd(@cmd_string);

  # ��recal.pl����TUI���õ�ʱ�򣬽ű������������������������߳�
  print "�����������, ����������߳�...\n";
 	
}

##################################################
#  �������ƣ� get_last_month
#  �������ܣ� ��ȡ��ǰ�µ��ϸ��¹������·�
#  ������  ��
##################################################
sub get_last_month{

  my $curr_year_month = `date +%Y%m`; 
  chomp($curr_year_month); 
  
  my $last_year_month = $curr_year_month - 1;
  my $last_month = substr("$last_year_month", 4); 
  
  if($last_month eq "00"){
    $last_year_month = $curr_year_month - 89;
  }
  
  return "$last_year_month";	
}

##################################################
#  �������ƣ� ssh_rm_remote
#  �������ܣ� ɾ��fts�ַ������ϵĲɼ��ļ�
#  ������ �߳�ID���ļ�����
##################################################
sub ssh_rm_remote 
{
	my ($thread_id, $current_file_type) = @_;
	
	$idx = 1;
	while (1) {
		$host = "SGIP".$idx;
		my $ip = $ENV{$host};
		if(not defined $ip or $ip eq "") {
			last;
		}
		if (substr($thread_id, 4, 4) gt "0250") {
			&exec_cmd("ssh $ip \"cd ~/Data/bps/$current_file_type/in;ls|egrep \"^\.\*1$current_file_type\\\.A1\$\"|xargs -i rm -rf {}\"");
		} else {
			&exec_cmd("ssh $ip \"cd ~/Data/bps/$current_file_type/in;ls|egrep \"^\.\*0$current_file_type\\\.A0\$\"|xargs -i rm -rf {}\"");
		}
		$idx ++;
	}
}
