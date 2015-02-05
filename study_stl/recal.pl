#!/usr/bin/perl -w
###############################################################################
# 脚本名称：recal.pl
# 功能：单线程重做，清空环境
# 使用方法：
#    recal.pl clear
#    recal.pl thread_id
#    recal.pl
# 参数：clear 表示全部清空佣金计算系统的工作目录和工作表，不包括支付的接口表
#       thread_id表示需要重做的线程号
#       无参数表示使用其交互式界面进行重算或者清空操作
###############################################################################

use warnings;
use Switch;
use POSIX;

sub exec_cmd;
sub main;
sub check_if_directory;

#设置日志文件
my $run_path = `pwd`;
chomp($run_path);
my $current_date = `date +%Y-%m-%d`;
chomp($current_date);
my $run_log_file;
$run_log_file = $run_path . "/log/recal-" . $current_date . ".log";

if (! -e $run_path . "/log"){
  print "目录不存在. 请创建目录: [$run_path/log].\n";
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
	print "环境变量未定义. [QUERY_USER]\n";
	exit 1;
}
my $QUERY_PASSWD = $ENV{'QUERY_PASSWD'};
if(not defined $QUERY_PASSWD or $QUERY_PASSWD eq ""){
	print "环境变量未定义. [QUERY_PASSWD]\n";
	exit 1;
}
my $QUERY_TNS = $ENV{'QUERY_TNS'};
if(not defined $QUERY_TNS or $QUERY_TNS eq ""){
	print "环境变量未定义. [QUERY_TNS]\n";
	exit 1;
}

my $CONN_STR = "$QUERY_USER/$QUERY_PASSWD\@$QUERY_TNS";

my $UNICAL_PROVINCE_CODE = "$ENV{'UNICAL_PROVINCE_CODE'}";
if(not defined $UNICAL_PROVINCE_CODE or $UNICAL_PROVINCE_CODE eq ""){
	print "环境变量未定义. [UNICAL_PROVINCE_CODE]\n";
	exit 1;
}

my $UNICAL_DT = "$ENV{'UNICAL_DT'}";
if(not defined $UNICAL_DT or $UNICAL_DT eq ""){
	print "环境变量未定义. [UNICAL_DT]\n";
	exit 1;
}

my $HOME = "$ENV{'AIOSS_HOME'}";
if(not defined $HOME or $HOME eq ""){
	print "环境变量未定义. [AIOSS_HOME]\n";
	exit 1;
}

my $ACCT_CYCLE = "$ENV{'ACCT_CYCLE'}";
if(not defined $ACCT_CYCLE or $ACCT_CYCLE eq ""){
	print "环境变量未定义. [ACCT_CYCLE]\n";
	exit 1;
}

my $RELEASE_BIN_PATH = "$HOME/debug/bin";
my $DEBUG_BIN_PATH = "$HOME/debug/bin";
my $HOST_ID = "$ENV{'UNICAL_HOST_ID'}";
if(not defined $HOST_ID or $HOST_ID eq ""){
	print "环境变量未定义. [UNICAL_HOST_ID]\n";
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

#调用主函数
&main;

##################################################
#   函数：main
#   功能：recal.pl 主函数入口
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
# 函数：tui_main
# 功能：recal.pl接收线程号作为参数，作为tui调用的主函数
#   当线程号用'clear'代替的时候，表示全清空操作   
##################################################
sub tui_main
{
	$TUI_THREAD_FLAG = 1;
	$TUI_THREAD = $ARGV[0];
	$TUI_FILE_TYPE = $ARGV[1];
	
	# 如果参数是 clear ，表示是TUI发送的清空环境的指令
	if(lc($TUI_THREAD) eq "clear"){
		&clear_all;
		exit 0;
	}
	
	# 如果是线程号，则继续执行                      
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
	  #佣金明细下发省分接口  
    if(index("03250100|03250101|03250102|03250103|03250104|03250105", $TUI_THREAD) >= 0){
      my $sql_id ="$UNICAL_PROVINCE_CODE" ."_del_ART_LTS";
      my $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n清除支付库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录的命令已经执行.\n";

      $sql_id ="$UNICAL_PROVINCE_CODE" ."_sel_ART_LTS";
      $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n支付库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录条数是[$msg].\n";
      if($msg eq "0") {
        print "清除支付库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录 成功.\n";
      }else{
        print "清除支付库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录 失败.\n";
      }
    }elsif(index("03700101|03800101|03100101", $TUI_THREAD) >= 0){
      my $sql_id = "$UNICAL_PROVINCE_CODE" ."_del_USG_LTS";
      my $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n清除计算库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录的命令已经执行.\n";

      $sql_id = "$UNICAL_PROVINCE_CODE" ."_sel_USG_LTS";
      $msg = `export aTHREAD_ID=$TUI_THREAD; cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n计算库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录条数是[$msg].\n";
      if($msg eq "0") {
        print "清除计算库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录 成功.\n";
      }else{
        print "清除计算库[LOG_THREAD_STATE]中本省[$TUI_THREAD]数据记录 失败.\n";
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
		&log("无效的 recal.pl 参数");
	};
}

##################################################
#   函数：interactive_main
#   功能：recal.pl脚本的交互式入口主函数
##################################################
sub interactive_main
{
  &print_welcome;
  &log("");
  &log("####################正常启动recal.pl脚本####################");

  my $choice = 0;
  while(1)
  {
    &print_menu_list("main");
    print "选择？\n";
    $choice = <STDIN>;
    chomp($choice);
    
    if($choice eq "1"){&redo_fts;}
    elsif($choice eq "2"){&redo_check;}
    elsif($choice eq "3"){&redo_unical;}
    elsif($choice eq "4"){&redo_stat;}
    elsif($choice eq "5"){&clear_all;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    else{print "无效，请重新选择！\n";}
  }
}

##################################################
#   函数：print_welcome
#   功能：recal.pl脚本的启动界面
##################################################
sub print_welcome
{
  system("clear");
  print "\n";
  print "#############################################################\n";
  print "#                                                           #\n";
  print "#                   佣金计算系统重算脚本                    #\n";
  print "#  脚本名称： recal.pl                                      #\n";
  print "#  脚本语言： perl                                          #\n";
  print "#  脚本功能： 实现佣金计算系统不同阶段的重算功能。          #\n";
  print "#  使用方法： 在 UNIX/LINUX 命令行直接输入脚本名称          #\n";
  print "#             启动即可，没有命令行参数。                    #\n";
  print "#                                                           #\n";
  print "#############################################################\n";
  print "\n";
}

##################################################
#   函数：log
#   功能：记录日志文件的函数
##################################################
sub log
{
	my $log_time = strftime("[%Y-%m-%d %H:%M:%S]:", localtime);
	my $log_content = shift;
  open  (LOG_FILE, ">>$run_log_file") or die "打开日志文件失败：$!\n";
  print LOG_FILE $log_time;
  print LOG_FILE $log_content,"\n";
  close LOG_FILE;
  
  print $log_time;
  $log_content eq ""?print "\n":print $log_content,"\n";
}

##################################################
#   函数：truct_table
#   功能：用于执行truncate table存储过程
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
		&log("【SQL操作】truncate table：【$_】");
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
  &log("【SQL操作】断开数据库连接");
}

##################################################
#   函数：truct_table_partition
#   功能：用于执行truncate table存储过程，清分区表
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
		&log("【SQL操作】truncate table：【$_】");
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
  &log("【SQL操作】断开数据库连接");
}

##################################################
#   函数：exec_sql
#   功能：执行数据库操作的函数
##################################################
sub exec_sql
{
  #&log("【SQL操作】开始数据库连接，连接串：【$CONN_STR】");
  open  ORA,"| sqlplus -s $CONN_STR " or die "$!\n";
  foreach (@_)
  {
    &log("【SQL操作】SQL语句：【$_】");
    print ORA "$_\n";
  }
  print ORA "commit;\n";
  print ORA "quit;\n";
  close ORA;
  &log("【SQL操作】断开数据库连接");
}

##################################################
#   函数：get_sql_result
#   功能：执行数据库操作的函数
##################################################
sub get_sql_result
{
	&log("【SQL操作】开始数据库连接，连接串：【$CONN_STR】");
  my $sql = shift;
  &log("【SQL操作】SQL语句：【$sql】");
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
  &log("【SQL操作】断开数据库连接");
  chomp($sql_result);
  $sql_result=~s/^\s+|\s+$//g;
  return ($sql_result);
}


##################################################
#   函数：exec_cmd
#   功能：执行操作系统的命令
##################################################
sub exec_cmd
{
  foreach (@_)
  {
    &log("【CMD操作】操作系统命令：【$_】");
    if (system("$_") != 0)
    {
      &log("【CMD操作】命令执行失败");
      &log("####################异常退出recal.pl脚本###################");
      exit 1;
    }
  }
}

##################################################
#   函数：kill_shm
#   功能：kil掉指点的shm
##################################################
sub kill_shm
{
	my $shm_thread = shift;
	my $thread_state = `ps -ef|grep "$USER_NAME"|grep "run_shm"|grep "$shm_thread"|grep -v "grep"`;
	chomp(my $proc_id = `echo '$thread_state'|awk '{print \$2}'`);
	if($proc_id ne ""){
		&log("线程号为：$shm_thread的共享内存将被Kill掉");
		exec_cmd("kill -INT $proc_id");
	}
}

##################################################
#   函数：is_running
#   功能：判断指定线程是否在运行，如果运行返回1，否则返回0
#   参数：线程号
##################################################
sub is_running
{
	my $thread = shift;
	if(not defined $thread){
		&log("函数is_running调用错误，没有提供参数！\n");
		exit 1;
	}
	my $thread_num = `ps -ef|grep "$USER_NAME"|grep "run_"|grep "$thread"|grep -v "grep"|wc -l`;
	chomp($thread_num);
	$thread_num =~ s/^\s+|\s+$//g;
	return $thread_num;
}

##################################################
#   函数：table_is_clear
#   功能：判断表或者视图中是否为空，如果为空返回真，否则返回假
#   参数：表名
##################################################
sub table_is_clear
{
	my $table_name = shift;
	if(not defined $table_name){
		&log("函数table_is_clear调用错误，没有提供参数！\n");
		exit 1;
	}
	$count = &get_sql_result("select count(*) from $table_name where rownum < 2");
	if(not defined $count){
		&log("函数table_is_clear错误，无法获得表[$table_name]是否为空");
		exit 1;
	}
	return $count == 0;
}

##################################################
#   函数：redo_fts
#   功能：重新采集接口数据入口函数
##################################################
sub redo_fts
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("fts");
    print "选择？\n";
    $choice = <STDIN>;
    chomp($choice);
    
    if($choice eq "r"){
    	return;
    } elsif($choice eq "q"){
    	&log("####################正常退出recal.pl脚本###################");
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
    	print "无效，请重新选择！\n";
    }
  }
  print "\n";
}

##################################################
#   函数：redo_fts_single_filetype
#   功能：启动重新采集接口数据线程的函数
#   参数：thread_id 线程号 file_type 文件类型
##################################################
sub redo_fts_single_filetype
{
  my ($thread_id, $current_file_type) = @_;
  my $choice = 0;
  
  my $thread_id_8_width = substr($thread_id, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 $current_file_type 采集日志表开始 ......");
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
    	print "无效，请重新选择！\n";
    }
  }
  
    &log("清除 $current_file_type 数据目录开始 .........");
    &exec_cmd("cd ${UNICAL_DT}/bps/$current_file_type/in;ls|egrep \"^\.\*0$current_file_type\\\.A0.gz\$\"|xargs -i rm -rf {}");
	if (substr($thread_id, 4, 2) eq "02") {
		&ssh_rm_remote($thread_id, $current_file_type);
	}
    &log("清除 $current_file_type 数据目录结束 .........");

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  &log("启动采集线程：$FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_fts -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'fts_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'fts_thread_id'} > /dev/null 2>&1 &");
}
##################################################
#   函数：redo_cbss_fts_single_filetype
#   功能：启动重新采集接口数据线程的函数
#   参数：thread_id 线程号 file_type 文件类型
##################################################
sub redo_cbss_fts_single_filetype
{
  my ($thread_id, $current_file_type) = @_;
  my $choice = 0;
  
  my $thread_id_8_width = substr($thread_id, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 $current_file_type 采集日志表开始 ......");
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
    	print "无效，请重新选择！\n";
    }
  }
  
    &log("清除 $current_file_type 数据目录开始 .........");
    &exec_cmd("cd ${UNICAL_DT}/bps/$current_file_type/in;ls|egrep \"^\.\*1$current_file_type\\\.A1\$\"|xargs -i rm -rf {}");
	if (substr($thread_id, 4, 2) eq "02") {
		&ssh_rm_remote($thread_id, $current_file_type);
	}
    &log("清除 $current_file_type 数据目录结束 .........");

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  &log("启动采集线程：$thread_id 开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_fts -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'fts_proc_id'} -t $thread_id eoc_tid$thread_id > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_check
#   功能：重新稽核接口数据入口函数
##################################################
sub redo_check
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("bps");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice >= 1 and $choice <= 21){&redo_check_single_filetype($FILE_TYPES[$choice-1]);}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
#   函数：redo_check_single_filetype
#   功能：启动重新稽核接口数据线程的函数
#   输入参数: $current_file_type 文件类型
##################################################
sub redo_check_single_filetype
{
  (my $current_file_type) = @_;
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  my $thread_id_8_width = substr($FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    	
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 [$current_file_type] 稽核日志表开始......");
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
      	&log("【检查操作】：表[DUP_BPS_$current_file_type]已经被清空");
      }else{
      	&log("【检查操作】：表[DUP_BPS_$current_file_type]未能清空成功，可以稍后再操作，或者请DBA查看表是否被锁定");
      	exit 1;
      }
      
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_file_type] 生成的共享内存文件开始 ......");
      if(not -e "$UNICAL_DT/shm/public/$ACCT_CYCLE/dat"){
        print "目录不存在,请创建.[$UNICAL_DT/shm/public/$ACCT_CYCLE/dat].命令未能成功执行.\n";
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
      	&log("$current_file_type不会生成共享内存文件，无须清理");
      }
      
      if($FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'} ne ""){
        &log("清空 [$current_file_type] 省份基础数据入库文件开始 ......");
        if(-e "$UNICAL_DT/stat/P$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/P$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[$UNICAL_DT/stat/P$current_file_type/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }
      
      if($current_file_type eq "NSDF"){
      	if(-e "$UNICAL_DT/stat/P$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/P$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[$UNICAL_DT/stat/P$current_file_type/in].命令未能成功执行.\n";
          exit 3;        	
        }
        
        if(-e "$UNICAL_DT/stat/$current_file_type/in"){
          &exec_cmd("cd $UNICAL_DT/stat/$current_file_type/in; ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[$UNICAL_DT/stat/$current_file_type/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }
      
      &log("清空 [$current_file_type] 生成的驱动源文件开始 ......");
      if($current_file_type eq "SERV"){
        if(-e "${UNICAL_DT}/unical/CRBO/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRBO/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRBO/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      
      }elsif($current_file_type eq "CARD"){
        if(-e "${UNICAL_DT}/unical/CRCS/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRCS/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRCS/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PAYO"){
        if(-e "${UNICAL_DT}/unical/CRCI/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRCI/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRCI/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "USER"){
        if(-e "${UNICAL_DT}/unical/CRUP/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRUP/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRUP/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PROM"){
        if(-e "${UNICAL_DT}/unical/CRPM/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPM/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPM/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "ORDR"){
        if(-e "${UNICAL_DT}/unical/CRPO/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPO/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPO/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "APAY"){
        if(-e "${UNICAL_DT}/unical/CRYC/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRYC/stage1/in;ls|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRYC/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "PFFA"){
        if(-e "${UNICAL_DT}/unical/CRPD/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage1/in;ls|egrep \"^\.\*PFFA\.\*\$\"|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPD/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }elsif($current_file_type eq "BUSI" ){
        if(-e "${UNICAL_DT}/unical/CRPD/stage1/in"){
          &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage1/in;ls|egrep \"^\.\*BUSI\.\*\$\"|xargs -i rm -rf {}");
        }else{
          print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPD/stage1/in].命令未能成功执行.\n";
          exit 3;        	
        }
      }else{
      	&log("[$current_file_type] 不会生成驱动源文件，无须清理");
      }
      
      &log("清空 [$current_file_type] 相关目录 [inv, dup, err, agg] 开始......");
      if(not -e "$UNICAL_DT/bps/$current_file_type/agg" or 
         not -e "$UNICAL_DT/bps/$current_file_type/dup" or
         not -e "$UNICAL_DT/bps/$current_file_type/err" or
         not -e "$UNICAL_DT/bps/$current_file_type/inv")
      {
        print "目录不存在,请创建.[$UNICAL_DT/bps/$current_file_type/agg][$UNICAL_DT/bps/$current_file_type/dup][$UNICAL_DT/bps/$current_file_type/err][$UNICAL_DT/bps/$current_file_type/inv].命令未能成功执行.\n";
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
    	print "无效，请重新选择！\n";
    }
  }

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'};
  }

  &log("启动共享内存线程：$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'shm_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'} > /dev/null 2>&1 &");
  
  print "开始sleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}_$COMM_CYCLE.log日志：\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$FILE_TYPE_INFO{$current_file_type}{'shm_thread_id'}加载数据完毕\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;


  &log("启动稽核线程：$FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_bps -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'bps_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'bps_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_unical
#   功能：重新计算指定驱动源入口函数
##################################################
sub redo_unical
{
  my $choice = 0;
  while(1)
  {
    &print_menu_list("unical");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&redo_unical_single_driver_stage1($DRIVER_TYPES[$choice-1]);}
    elsif($choice eq "10"){&redo_unical_single_driver_stage2;}
    elsif($choice >= 11 and $choice <= 19){&redo_unical_single_driver_stage3($DRIVER_TYPES[$choice-11]);}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
#   函数：redo_unical_single_driver_stage1
#   功能：启动重新计算第一阶段线程的函数
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
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_driver_type] 驱动源第一阶段的日志表开始 ......");
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
      
      #清空账户级明细的查重表
      $sql_id = "$UNICAL_PROVINCE_CODE" ."_del_UNIDUP";
      $msg = `cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      print $msg . "\n清除计算库[DUP_UNIPRE]中数据记录的命令已经执行.\n";

      $sql_id = "$UNICAL_PROVINCE_CODE" ."_sel_UNIDUP";
      $msg = `cd $RELEASE_BIN_PATH; exec_sql_encrypt -i $sql_id`;
      chomp($msg);chomp($msg);chomp($msg);
      print $msg . "\n计算库[DUP_UNIPRE]中数据记录条数是[$msg].\n";
      if($msg eq "0") {
        print "清除计算库[DUP_UNIPRE] 成功.\n";
      }else{
        print "清除计算库[DUP_UNIPRE] 失败.\n";
      }
      
      last;
    }elsif($choice eq "n" or $choice eq "N"){
      last;
    }else{
    	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_driver_type] 第一阶段相关目录开始 ......");
      if(not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/dup" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/err" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage1/inv" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/in"  or
         not -e "${UNICAL_DT}/busi/in")
      {
        print "目录不存在,请创建.[${UNICAL_DT}/unical/$current_driver_type/stage1/dup][${UNICAL_DT}/unical/$current_driver_type/stage1/err][${UNICAL_DT}/unical/$current_driver_type/stage1/inv][${UNICAL_DT}/unical/$current_driver_type/stage3/in][${UNICAL_DT}/busi/in].命令未能成功执行.\n";
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
    	print "无效，请重新选择！\n";
    }
  }
  
  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'};
  }

  &log("启动共享内存线程：$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'} > /dev/null 2>&1 &");

  print "开始sleep 3 Seconds...\n";
  sleep(3);
  print "$HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}_$COMM_CYCLE.log日志：\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_shm_thread_id'}加载数据完毕\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("启动佣金计算第一阶段线程：$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unipre_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_unical_single_driver_stage2
#   功能：启动重新计算第二阶段线程的函数
##################################################
sub redo_unical_single_driver_stage2
{
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  
  if(is_running("19200101")){
  	&log("[19200101]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空佣金计算第二步的日志表开始 ......");
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
    	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空第二阶段相关目录开始 ......");
      if(not -e "${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/shm/public/${ACCT_CYCLE}/dat].命令未能成功执行.\n";
        exit 3;      
      }
      if(not -e "${UNICAL_DT}/busi/his" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/busi/his].命令未能成功执行.\n";
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
    	print "无效，请重新选择！\n";
    }
  }
  
  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne "01300101";
  }

  &log("启动共享内存线程：01300101开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p 30 -t 01300101 eoc_tid01300101 > /dev/null 2>&1 &");

  print "开始sleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/01300101_$COMM_CYCLE.log日志：\n";
  my $pid = open HD,"tail -f $HOME/log/shm/01300101_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "01300101加载数据完毕\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("启动佣金计算第二阶段线程：19200101开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p 20 -t 19200101 eoc_tid19200101 > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_unical_single_driver_stage3
#   功能：启动重新计算第三阶段线程的函数
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
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空$current_driver_type驱动源第三阶段的日志表开始......");
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
    	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_driver_type] 第三阶段相关目录开始 ......");
      if(not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/dup" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/err" or
         not -e "${UNICAL_DT}/unical/$current_driver_type/stage3/inv" or
         not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in")
      {
        print "目录不存在,请创建.[${UNICAL_DT}/unical/$current_driver_type/stage3/dup][${UNICAL_DT}/unical/$current_driver_type/stage3/err]".
              "[${UNICAL_DT}/unical/$current_driver_type/stage3/inv][${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/in].命令未能成功执行.\n";
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
      
      &log("清空 [$current_driver_type] 生成的冻结文件开始 ......");
      if(not -e "${UNICAL_DT}/stat/SSFD/in" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/stat/SSFD/in].命令未能成功执行.\n";
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
    	print "无效，请重新选择！\n";
    }
  }
  
  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;
  
  foreach (@shm_seq){
  	&kill_shm("$_") if $_ ne $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'};
  }

  &log("启动共享内存线程：$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_shm -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'} > /dev/null 2>&1 &");

  print "开始sleep 3 Seconds...\n";
  sleep (3);
  print "$HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}_$COMM_CYCLE.log日志：\n";
  my $pid = open HD,"tail -f $HOME/log/shm/$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}_$COMM_CYCLE.log|";
  while(<HD>){
    print $_;
    if($_=~/sleep/){ 
    	print "$DRIVER_TYPE_INFO{$current_driver_type}{'unical_shm_thread_id'}加载数据完毕\n"; 
    	kill INT => $pid;
    	kill HUP => $pid;
    	close(HD);
    	last; 
    }
  }
  close HD;

  &log("启动佣金计算第三阶段线程：$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_unical -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'unical_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'unical_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_stat
#   功能：重新入库的入口函数
##################################################
sub redo_stat
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("sub_stat");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice eq "1"){&redo_stat_prov;}
    elsif($choice eq "2"){&redo_stat_comm;}
    elsif($choice eq "3"){&redo_stat_frz;}
    elsif($choice eq "4"){&redo_stat_sett;}
    elsif($choice eq "5"){&redo_stat_comp;}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
#   函数：redo_stat_prov
#   功能：省分原始数据重新入库的入口函数
##################################################
sub redo_stat_prov
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_prov");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice >= 1 and $choice <= 20){&stat_prov($FILE_TYPES[$choice-1]);}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
# 函数：stat_prov
# 功能：省分原始数据重新入库的实现函数
# 参数： thread_id 线程号 current_file_type 接口文件类型
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

  #获取实际的佣金账期$actrual_cycle，即根据日历计算出来的账期，跟环境变量中的账期$acct_cycle进行比较，如果不一致，则说明要同步历史账期的数据，则不允许
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "根据系统时间推算当前账期是[$actrual_cycle], 但环境变量设置的账期是[$ACCT_CYCLE], 不能执行重入库命令。\n";
    print "该命令本意是把当前账期的基础数据进行重新入库操作，即只能操作当前账期的数据。\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除省分原始数据重新入库数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 [$current_stat_key] 入库日志表开始......");
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
    	print "无效，请重新选择！\n";
    }
  }
  
  if($current_stat_key ne "NSDF" and $current_stat_key ne "NPRE"){
    while(1){
    	if(!$TUI_THREAD_FLAG){
        print "清除数据表 [$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE]（y|Y清除,n|N不清除）？\n";
        $choice=<STDIN>;
        chomp($choice);
      }else{$choice = "y";}   
    
      if($choice eq "y" or $choice eq "Y"){
        &log("清除 [$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE] 入库数据表开始 ......");
        &trunc_table("$FILE_TYPE_INFO{$current_file_type}{'prov_stat_table'}\_$ACCT_CYCLE");
        last;
      }elsif($choice eq "n" or $choice eq "N"){
        last;
      }else{
      	print "无效，请重新选择！\n";
      }
    }
  }
  
  if($current_stat_key eq "PPFFA"){
    while(1){
    	if(!$TUI_THREAD_FLAG){
        print "清除数据表 [PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE]（y|Y清除,n|N不清除）？\n";
        $choice=<STDIN>;
        chomp($choice);
      }else{$choice = "y";}   
    
      if($choice eq "y" or $choice eq "Y"){
        &log("清除 [PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE] 入库数据表开始 ......");
        &trunc_table("PROV_PFFA_CHNL_SUM\_$ACCT_CYCLE");
        last;
      }elsif($choice eq "n" or $choice eq "N"){
        last;
      }else{
      	print "无效，请重新选择！\n";
      }
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_stat_key] 相关目录开始 ......");
      if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/stat/$current_stat_key/dup].命令未能成功执行.\n";
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
       print "无效，请重新选择！\n";
    }
  }
  
  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;

  &log("启动入库线程：$FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $FILE_TYPE_INFO{$current_file_type}{'prov_stat_proc_id'} -t $FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'} eoc_tid$FILE_TYPE_INFO{$current_file_type}{'prov_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_stat_comm
#   功能：佣金计算明细重新汇总的入口函数
##################################################
sub redo_stat_comm
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_comm");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&stat_comm($DRIVER_TYPES[$choice-1]);}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
#   函数：stat_comm
#   功能：佣金计算明细重新汇总的实现函数
##################################################
sub stat_comm
{
  my ($current_driver_type) = @_;
  my $current_stat_key = "COMM";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;

  #获取实际的佣金账期$actrual_cycle，即根据日历计算出来的账期，跟环境变量中的账期$acct_cycle进行比较，如果不一致，则说明要同步历史账期的数据，则不允许
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "根据系统时间推算当前账期是[$actrual_cycle], 但环境变量设置的账期是[$ACCT_CYCLE], 不能执行佣金明细重入库命令。\n";
    print "该命令本意是把当前账期的佣金明细数据进行重新入库操作，即只能操作当前账期的数据。\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除佣金计算明细汇总数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 [$current_stat_key] 入库日志表开始 ......");
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
      	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice=<STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_stat_key] 相关目录开始 ......");
      if(not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup].命令未能成功执行.\n";
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
      	print "无效，请重新选择！\n";
    }
  }

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;

  &log("启动入库线程：$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'comm_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_stat_frz
#   功能：冻结明细重新入库的入口函数
##################################################
sub redo_stat_frz
{
  &stat_frz;
  print "\n";
}

##################################################
#   函数：stat_frz
#   功能：冻结明细重新入库的实现函数
##################################################
sub stat_frz
{
  my $current_stat_key = "SSFD";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;
  
  #获取实际的佣金账期$actrual_cycle，即根据日历计算出来的账期，跟环境变量中的账期$acct_cycle进行比较，如果不一致，则说明要同步历史账期的数据，则不允许
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "根据系统时间推算当前账期是[$actrual_cycle], 但环境变量设置的账期是[$ACCT_CYCLE], 不能执行冻结明细重入库命令。\n";
    print "该命令本意是把当前账期的冻结明细数据进行重新入库操作，即只能操作当前账期的数据。\n";
    exit 1;  
  }
  
  if(is_running("14600101")){
  	&log("[14600101]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 [$current_stat_key] 入库日志表开始 ......");
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
      	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空$current_stat_key相关目录开始......");
      if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/stat/$current_stat_key/dup].命令未能成功执行.\n";
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
      	print "无效，请重新选择！\n";
    }
  }

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;

  &log("启动入库线程：14600101开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p 60 -t 1460010110 eoc_tid14600101 > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_stat_comp
#   功能：复合指标重新入库的入口函数
##################################################
sub redo_stat_comp
{
  &stat_comp;
  print "\n";
}

##################################################
#   函数：stat_comp
#   功能：复合指标重新入库的实现函数
##################################################
sub stat_comp
{
  my $current_stat_key = "COMP";
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  #获取实际的佣金账期$actrual_cycle，即根据日历计算出来的账期，跟环境变量中的账期$acct_cycle进行比较，如果不一致，则说明要同步历史账期的数据，则不允许
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "根据系统时间推算当前账期是[$actrual_cycle], 但环境变量设置的账期是[$ACCT_CYCLE], 不能执行复合指标重入库命令。\n";
    print "该命令本意是把当前账期的复合指标数据进行重新入库操作，即只能操作当前账期的数据。\n";
    exit 1;  
  }
  
  if(is_running("14610101")){
  	&log("[14610101]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清除 [$current_stat_key] 入库日志表开始 ......");
      &log("清除复合指标入库的日志表以及数据表......");
      `cd $RELEASE_BIN_PATH;exec_sql_encrypt -i delete_comp`;
      last;
    }elsif($choice eq "n" or $choice eq "N"){
        last;
    }else{
      	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      ## &log("清空$current_stat_key相关目录开始......");
      ## if(not -e "${UNICAL_DT}/stat/$current_stat_key/dup" )
      ## {
      ##   print "目录不存在,请创建.[${UNICAL_DT}/stat/$current_stat_key/dup].命令未能成功执行.\n";
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
      	print "无效，请重新选择！\n";
    }
  }

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;

  &log("启动入库线程：14610101开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p 60 -t 1461010110 eoc_tid14610101 > /dev/null 2>&1 &");
}

##################################################
#   函数：redo_stat_sett
#   功能：重新下账的入口函数
##################################################
sub redo_stat_sett
{
  my $choice=0;
  while(1)
  {
    &print_menu_list("stat_sett");
    print "选择？\n";
    $choice=<STDIN>;
    chomp($choice);
    
    if($choice eq "r"){return;}
    elsif($choice eq "q"){&log("####################正常退出recal.pl脚本###################");exit 0;}
    elsif($choice >= 1 and $choice <= 9){&stat_sett($DRIVER_TYPES[$choice-1]);}
    else{print "无效，请重新选择！\n";}
  }
  print "\n";
}

##################################################
#   函数：stat_sett
#   功能：重新下账的实现函数
##################################################
sub stat_sett
{
  (my $current_driver_type) = @_;
  my $current_stat_key = "SETT";
  my @sql_string;
  my @cmd_string;
  my @cmd_mv;
  my $choice = 0;

  #获取实际的佣金账期$actrual_cycle，即根据日历计算出来的账期，跟环境变量中的账期$acct_cycle进行比较，如果不一致，则说明要同步历史账期的数据，则不允许
  my $actrual_cycle = &get_last_month;
  if("$ACCT_CYCLE" ne "$actrual_cycle" ){
    print "根据系统时间推算当前账期是[$actrual_cycle], 但环境变量设置的账期是[$ACCT_CYCLE], 不能执行重下账命令。\n";
    print "该命令本意是把当前账期的佣金数据进行重下账操作，即只能操作当前账期的数据。\n";
    exit 1;  
  }
  
  my $thread_id_8_width = substr($DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}, 0, 8);
  if(is_running($thread_id_8_width)){
  	&log("[$thread_id_8_width]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除数据库日志（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){                     
      &log("清除 [$current_stat_key] 入库日志表开始 ......");
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
      	print "无效，请重新选择！\n";
    }
  }

  while(1){
  	if(!$TUI_THREAD_FLAG){
      print "清除输出，无效，错误等文件（y|Y清除,n|N不清除）？\n";
      $choice = <STDIN>;
      chomp($choice);
    }else{$choice = "y";}   
    
    if($choice eq "y" or $choice eq "Y"){
      &log("清空 [$current_stat_key] 相关目录开始 ......");
      if(not -e "${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2" )
      {
        print "目录不存在,请创建.[${UNICAL_DT}/stat/$DRIVER_TYPE_INFO{$current_driver_type}{'output_stat_dir'}/dup2].命令未能成功执行.\n";
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
      	print "无效，请重新选择！\n";
    }
  }

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
  #print "环境清理完毕, 准备启动相关线程...\n";
  return if $TUI_THREAD_FLAG;

  &log("启动入库线程：$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'}开始......");
  &exec_cmd("cd $RELEASE_BIN_PATH;nohup run_stat -h $HOST_ID -p $DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_proc_id'} -t $DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'} eoc_tid$DRIVER_TYPE_INFO{$current_driver_type}{'sett_stat_thread_id'} > /dev/null 2>&1 &");
}

##################################################
#   函数：check_if_directory
#   功能：检查是否为目录
#   参数：传入的名称
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
#   函数：recursive_traversal_remove_file
#   功能：递归遍历一个目录，将该目录下面所有的文件删除掉，所有子目录里面的文件也会被删除掉
#   参数：要删除文件的根目录
##################################################
sub recursive_traversal_remove_file
{
	#获取递归的目录
  my $root = $_[0]; 
  my $dir_handle = sprintf("DH%d", ++$globle_index);
  opendir $dir_handle, $root or die "Can't open directory,information:$!!\n";
  my @dirs = readdir $dir_handle;
  foreach(@dirs){
    #if(check_if_directory("$root/$_")){  #如果是目录
    if(-d "$root/$_"){  #如果是目录
    	if(not /^(\.|\.\.)$/){
        &log("开始清理目录$root/$_");
        recursive_traversal_remove_file("$root/$_") if (not /^(\.|\.\.)$/);
      }
    }else{    #如果是文件
      &exec_cmd("rm -f $root/$_");
    }
  }
  
  closedir $dir_handle;
}

##################################################
#   函数：get_object_name
#   功能：从数据库中获取指定匹配模式的对象名称
#   参数：匹配模式
##################################################
sub get_object_name
{
	my $pattern = shift;
	my $object_names = &get_sql_result("select object_name from user_objects where object_name like '$pattern'");
	my @object_names = split(/\n/, $object_names);
	return @object_names;
}

##################################################
#   函数：clear_all
#   功能：清理佣金计算所有相关目录文件
#   参数：无
##################################################
sub clear_all
{
	my $choice = 0;
	if(!$TUI_THREAD_FLAG){
	  print "该操作将会删除工作环境[$UNICAL_DT]下面所有文件\n";
	  print "该操作将会清空[$CONN_STR]下所有数据采集，稽核，计算，入库所有环节的日志表\n";
	  print "请谨慎使用！！\n";
	  print "确认要进行该操作？（输入YES确定）\n";
	  $choice = <STDIN>;
	  chomp($choice);
	}else{$choice = "YES"};
		
	if($choice eq "YES"){
	  print "该操作将会删除工作环境[$UNICAL_DT]下面所有文件\n";
		print "please wait ...\n";
		&log("开始递归删除 $UNICAL_DT 目录下的所有文件 ......");
    &recursive_traversal_remove_file("$UNICAL_DT");
    
    &log("开始递归删除 $HOME/log 目录下的所有文件 ......");
    &recursive_traversal_remove_file("$HOME/log");
    
    &log("开始递归删除 $HOME/debug/log 目录下的所有文件 ......");
    &recursive_traversal_remove_file("$HOME/debug/log");
    
    #&clear_table_truncate();  #需要使用 truncate 语句的时候，使用这个函数
    &clear_table_province();  #需要按省清空表的时候，使用这个函数，这个函数使用 delete 语句
    
    
  }else{
  	print "操作取消!\n";
  	return;
  }
}
##################################################
#   函数： clear_table_province
#   功能： 使用 delete 语句按省把表清空
#   参数： 无
##################################################
sub clear_table_province
{
	my @sql_string;
	
  &log("开始按省清空 exp 的日志表 ...");
  @sql_string = (
  "delete from LOG_EXP where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from EXP_BREAK_POINT where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);
	
  &log("开始按省清空 fts 的日志表 ...");
  @sql_string = (
  "delete from LOG_FTS_PROV where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_FTS_NS   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_FTS_RR   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from FTS_BREAK_POINT where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);

  # BPS 组件的日志表的清空
  &log("开始按省清空 bps 的日志表 ...");
  @sql_string = (
  "delete from LOG_BPS where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_BPS_LIST   where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);

  &log("开始按省清空 bps 的查重工作表 ...");
  @object_names = &get_object_name("DUP_BPS\%");
  &trunc_table(@object_names);
 
  @object_names = &get_object_name("ERR_BPS\%");
  &trunc_table(@object_names);
    
  # UNICAL 组件的日志表的清空
  &log("开始清空 unical 的日志表...");
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
      
    
  # STAT 组件的日志表的清空
  &log("开始清空 stat 的日志表...");
  @sql_string = (
  "delete from LOG_STAT_MON where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';",
  "delete from LOG_STAT_DAY where PROVINCE_CODE = \'$UNICAL_PROVINCE_CODE\';"
  );
  &exec_sql(@sql_string);
    
    &log("开始清空其他的日志表...");
    @object_names = ();
    push(@object_names,"LOG_THREAD_STATE");
    #push(@object_names,"LOG_UNICAL_STAT");
    #push(@object_names,"ERROR_REPORT");
    #push(@object_names,"LOG_PROVFEE_STAT");
    #push(@object_names,"LOG_PROVFILE_MON");
    &trunc_table(@object_names);
}

##################################################
#   函数： clear_table_truncate
#   功能： 使用 truncate 语句把表清空
#   参数： 无
##################################################
sub clear_table_truncate
{
    my @object_names;
    
    &log("开始清空exp的日志表...");
    @object_names = ();
    push(@object_names, "LOG_EXP");
    push(@object_names, "EXP_BREAK_POINT");
    &trunc_table(@object_names);
    
    &log("开始清空fts的日志表...");
    @object_names = &get_object_name("LOG_FTS\%");
    push(@object_names,"FTS_BREAK_POINT");
    &trunc_table(@object_names);
    
    &log("开始清空bps的日志表...");
    @object_names = &get_object_name("LOG_BPS\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("ERR_BPS\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("DUP_BPS\%");
    &trunc_table(@object_names);
    
    &log("开始清空unical的日志表...");
    @object_names = &get_object_name("LOG_UNI\%");
    &trunc_table(@object_names);
    
    @object_names = &get_object_name("ERR_UNI\%");
    &trunc_table(@object_names);
    
    &log("开始清空stat的日志表...");
    @object_names = &get_object_name("LOG_STAT\%");
    &trunc_table(@object_names);
    
    &log("开始清空其他的日志表...");
    @object_names = ();
    push(@object_names,"LOG_THREAD_STATE");
    push(@object_names,"LOG_UNICAL_STAT");
    #push(@object_names,"ERROR_REPORT");
    #push(@object_names,"LOG_PROVFEE_STAT");
    #push(@object_names,"LOG_PROVFILE_MON");
    &trunc_table(@object_names);
	
}

##################################################
#   函数：print_menu_list
#   功能：打印各种菜单和列表
##################################################
sub print_menu_list
{
  (my $flag) = (@_);
  my $file_type;
  my $index = 0;
  print "-"x50, "\n";
  
  if($flag eq "main"){
  	print " "x20,"主菜单\n";
    print "- 1  重新采集\n";
    print "- 2  重新稽核\n";
    print "- 3  重新计算\n";
    print "- 4  重新入库\n";
    print "- 5  清理环境（慎用）！\n";
  }
  elsif($flag eq "fts"){
  	print " "x20, "重新采集主菜单\n";
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
  	print " "x20,"重新稽核主菜单\n";
    for $file_type(@FILE_TYPES)
    {
      printf "- %02d $file_type接口\n",++$index;
    }
  }
  elsif($flag eq "unical"){
  	print " "x20,"重新计算主菜单\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type第一阶段\n",++$index;
    }
    printf "- %02d 佣金计算第二阶段\n",++$index;
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type第三阶段\n",++$index;
    }
  }
  elsif($flag eq "sub_stat"){
  	print " "x20,"重新入库主菜单\n";
    print "- 1  省分原始数据重新入库\n";
    print "- 2  佣金计算明细重新汇总\n";
    print "- 3  冻结明细重新入库\n";
    print "- 4  重新下账\n";
    print "- 5  复合指标重新入库\n";
  }
  elsif($flag eq "stat_prov"){
  	print " "x20,"省分原始数据重新入库主菜单\n";
    for $file_type(@FILE_TYPES)
    {
      printf "- %02d P$file_type接口\n",++$index;
    }
  }
  elsif($flag eq "stat_comm"){
  	print " "x20,"佣金计算明细重新汇总主菜单\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type入库\n",++$index;
    }
  }
  elsif($flag eq "stat_sett"){
  	print " "x20,"重新下账主菜单\n";
    for $file_type(@DRIVER_TYPES)
    {
      printf "- %02d $file_type下账\n",++$index;
    }
  }

  if($flag ne "main")
  {
    print "- r  返回上层目录\n";
  }
  print "- q  退出!\n";
  print "-"x50,"\n";
}

##################################################
#   函数： redo_13900101
#   功能： 对13900101线程进行重做，没有输入参数
##################################################
sub redo_13900101
{
  my @sql_string;
  my @cmd_string;
  my $choice = 0;
  
  if(is_running("13900101")){
  	&log("[13900101]线程正在运行，此时不能执行[REDO]操作，请先停止线程，然后再执行[REDO]操作");
  	exit 1;
  }
  
  &log("清除 [FRZD] 稽核日志表开始......");
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
  	&log("【检查操作】：表[err_bps_FRZD]已经被清空");
   }else{
   	&log("【检查操作】：表[err_bps_FRZD]未能清空成功，可以稍后再操作，或者请DBA查看表是否被锁定");
  	exit 1;
  }
      
  &log("清空 [FRZD] 生成的解冻文件开始 ......");
  if(-e "${UNICAL_DT}/unical/CRBO/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRBO/stage3/in;ls|egrep \"^\.\*SERV\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRBO/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }
  
  if(-e "${UNICAL_DT}/unical/CRCS/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCS/stage3/in;ls|egrep \"^\.\*CARD\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRCS/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRCI/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCI/stage3/in;ls|egrep \"^\.\*PAYO\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRCI/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRUP/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRUP/stage3/in;ls|egrep \"^\.\*USER\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRUP/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPM/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPM/stage3/in;ls|egrep \"^\.\*PROM\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPM/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPO/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPO/stage3/in;ls|egrep \"^\.\*ORDR\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPO/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRYC/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRYC/stage3/in;ls|egrep \"^\.\*APAY\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRYC/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }

  if(-e "${UNICAL_DT}/unical/CRPD/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRPD/stage3/in;ls|egrep \"^\.\*PFFA\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRPD/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }
  if(-e "${UNICAL_DT}/unical/CRCH/stage3/in"){
    &exec_cmd("cd ${UNICAL_DT}/unical/CRCH/stage3/in;ls|egrep \"^\.\*CHNL\.\*\\\.Z0\\\.\.\*\$\"|xargs -i rm -rf {}");
  }else{
    print "目录不存在,请创建.[${UNICAL_DT}/unical/CRCH/stage3/in].命令未能成功执行.\n";
    exit 3;        	
  }
      
  &log("清空 [FRZD] 相关目录 [inv, dup, err, agg] 开始......");
  if(not -e "$UNICAL_DT/bps/FRZD/agg" or 
     not -e "$UNICAL_DT/bps/FRZD/dup" or
     not -e "$UNICAL_DT/bps/FRZD/err" or
     not -e "$UNICAL_DT/bps/FRZD/inv")
  {
    print "目录不存在,请创建.[$UNICAL_DT/bps/FRZD/agg][$UNICAL_DT/bps/FRZD/dup][$UNICAL_DT/bps/FRZD/err][$UNICAL_DT/bps/FRZD/inv].命令未能成功执行.\n";
    exit 3;        	
  }
  
  @cmd_string = (
  "cd $UNICAL_DT/bps/FRZD/agg; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/dup; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/err; ls|xargs -i rm -rf {}",
  "cd $UNICAL_DT/bps/FRZD/inv; ls|xargs -i rm -rf {}"
  );
  &exec_cmd(@cmd_string);

  # 当recal.pl是由TUI调用的时候，脚本的作用是清理环境，不启动线程
  print "环境清理完毕, 请启动相关线程...\n";
 	
}

##################################################
#  函数名称： get_last_month
#  函数功能： 获取当前月的上个月归属的月份
#  参数：  无
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
#  函数名称： ssh_rm_remote
#  函数功能： 删除fts分发主机上的采集文件
#  参数： 线程ID，文件类型
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
