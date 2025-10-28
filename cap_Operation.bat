@echo off
setlocal enabledelayedexpansion

rem 设置默认的vision标志为false
set "vision=false"

rem 解析命令行参数
:parse_args
if "%~1"=="-v" (
    set "vision=true"
    shift
    goto :parse_args
)

rem 如果启用了vision模式，显示详细信息
if "!vision!"=="true" (
    echo Vision mode enabled
)

rem 运行Python脚本并获取CSV文件路径
echo 正在运行数据预处理脚本...
for /f "delims=" %%i in ('python data_PreProcessing_V1.py ^| findstr /r "已成功将csvFiles_for_CapOperation.csv复制到:"') do (
    rem 提取路径部分（假设路径在冒号后面）
    for /f "tokens=2* delims=:" %%a in ("%%i") do (
        set "CSV_PATH=%%b"
        rem 去除路径前后的空格
        set "CSV_PATH=!CSV_PATH:~1!"
    )
)

rem 给CSV_PATH补充D:\
set "CSV_PATH=D:\!CSV_PATH!"

rem 检查是否成功获取路径
if defined CSV_PATH (
     if exist "%CSV_PATH%" (
        echo Successfully retrieved CSV file path: !CSV_PATH! and confirmed its existence
        rem type "!CSV_PATH!"

        rem 获取CSV文件总共有多少行（包括标题行）
        call :CountCSVLines "!CSV_PATH!" TOTAL_LINES
        echo Total lines in CSV file: !TOTAL_LINES!
        echo Expected loop range: 2 to !TOTAL_LINES!

        rem 记录各个列名的索引
        set "ID_COLUMN_INDEX=1"
        set "SRC_ADD_COLUMN_INDEX=2"
        set "SCENE_COLUMN_INDEX=3"
        set "LIB_ADD_COLUMN_INDEX=4"
        set "LOCAL_IP_COLUMN_INDEX=5"
        set "SERV_IP_COLUMN_INDEX=6"
        set "START_TIME_COLUMN_INDEX=7"
        set "END_TIME_COLUMN_INDEX=8"
        set "CAPFILE_ADD_COLUMN_INDEX=9"

        rem 遍历所有行（从第2行开始，跳过标题行）
        @REM echo Starting loop from row 2 to !TOTAL_LINES!
        for /l %%i in (2,1,!TOTAL_LINES!) do (
            echo Processing row %%i of !TOTAL_LINES!
            rem 首先读取每一行中各个列的内容
            call :ReadCSVCell "!CSV_PATH!" %%i !ID_COLUMN_INDEX! ID_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !SRC_ADD_COLUMN_INDEX! SRC_ADD_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !SCENE_COLUMN_INDEX! SCENE_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !LIB_ADD_COLUMN_INDEX! LIB_ADD_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !LOCAL_IP_COLUMN_INDEX! LOCAL_IP_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !SERV_IP_COLUMN_INDEX! SERV_IP_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !START_TIME_COLUMN_INDEX! START_TIME_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !END_TIME_COLUMN_INDEX! END_TIME_VALUE
            call :ReadCSVCell "!CSV_PATH!" %%i !CAPFILE_ADD_COLUMN_INDEX! CAPFILE_ADD_VALUE
            rem 打印当前行的CAPFILE_ADD_COLUMN_INDEX列的内容
            @REM echo Content of CAPFILE_ADD_COLUMN_INDEX column in row %%i: !CAPFILE_ADD_VALUE!
            if %%i equ !TOTAL_LINES! echo This is the last row according to loop settings
            rem 检查CAPFILE_ADD_VALUE指向的目录是否存在
            if not exist "!CAPFILE_ADD_VALUE!" (
                echo Error: capfile in row %%i is not Valid!
                exit /b 1
            )
            rem 检查CAPFILE_ADD_VALUE指向的目录当中是否有.pcap*后缀的文件存在
            set "CAPFILE_EXIST=false"
            rem 不使用goto，让循环自然结束
            for %%f in ("!CAPFILE_ADD_VALUE!\*.pcap*") do (
                set "CAPFILE_EXIST=true"
                rem 即使找到了文件，也让循环继续，这不会影响结果
            )
            if "!CAPFILE_EXIST!"=="false" (
                echo Error: CAPFILE_ADD_VALUE in row %%i does not contain any .pcap* file!
                exit /b 1
            )
            rem 检查SRC_ADD_VALUE或LIB_ADD_VALUE的内容是否为空
            if "!SRC_ADD_VALUE!"=="" (
                echo Error: SRC_ADD_VALUE in row %%i is empty!
                exit /b 1
            )
            if "!LIB_ADD_VALUE!"=="" (
                echo Error: LIB_ADD_VALUE in row %%i is empty!
                exit /b 1
            )
            rem cap源文件内容确定，IP地址确定，准备工作完毕，检测仓库目录是否正常
            if not exist "!LIB_ADD_VALUE!" (
                echo Error: LIB_ADD_VALUE in row %%i is not Valid!
                exit /b 1
            )

            rem 仓库目录存在，开始抓取操作
            set "LOCAL_IP_PARTS=0"
            rem 检查LOCAL_IP_VALUE是否为空
            if not "!LOCAL_IP_VALUE!"=="" (
                rem 如果字符串不包含+分隔符，那么整个字符串就是一个部分
                if "!LOCAL_IP_VALUE!"=="!LOCAL_IP_VALUE:+="."!" (
                    set /a LOCAL_IP_PARTS=1
                    set "LOCAL_IP_PART_1=!LOCAL_IP_VALUE!"
                ) else (
                    rem 字符串包含+分隔符，需要分割处理
                    set "temp=!LOCAL_IP_VALUE!"
                    set /a LOCAL_IP_PARTS=0
                
                    call :split_local_ip "!temp!" LOCAL_IP_PARTS LOCAL_IP_PART_
                )
            )
            echo LOCAL_IP_PARTS: !LOCAL_IP_PARTS!
            rem 输出每个分隔部分的内容
            for /l %%n in (1,1,!LOCAL_IP_PARTS!) do (
                echo LOCAL_IP_PART_%%n: !LOCAL_IP_PART_%%n!
            )
            
            rem 检查SERV_IP是否为空
            if not "!SERV_IP_VALUE!"=="" (
                rem 如果字符串不包含+分隔符，那么整个字符串就是一个部分
                if "!SERV_IP_VALUE!"=="!SERV_IP_VALUE:+="."!" (
                    set /a SERV_IP_PARTS=1
                    set "SERV_IP_PART_1=!SERV_IP_VALUE!"
                ) else (
                    rem 字符串包含+分隔符，需要分割处理
                set "temp=!SERV_IP_VALUE!"
                set /a SERV_IP_PARTS=0
                
                rem 使用call命令创建一个子环境来处理分割，避免goto影响外部循环
                call :split_serv_ip "!temp!" SERV_IP_PARTS SERV_IP_PART_
                )
            )
            echo SERV_IP_PARTS: !SERV_IP_PARTS!
            rem 输出每个分隔部分的内容
            for /l %%n in (1,1,!SERV_IP_PARTS!) do (
                echo SERV_IP_PART_%%n: !SERV_IP_PART_%%n!
            )
            rem 如果IP内容为空，进行报警
            if !LOCAL_IP_PARTS! lss 1 (
                echo Error: LOCAL_IP_VALUE in row %%i is not Valid!
                exit /b 1
            )

            rem 在LIB_ADD_VALUE目录下创建CAP文件处理之后的文件
            set "CAPED_FILE_DIR=!LIB_ADD_VALUE!\result_form_capFile"
            if not exist "!CAPED_FILE_DIR!" mkdir "!CAPED_FILE_DIR!"
            rem 遍历CAP_FILE_ADD_VALUE目录下的所有文件
            set "num=0"
            for %%f in ("!CAPFILE_ADD_VALUE!\*.pcap*") do (
                rem 打印当前处理的文件名
                @REM echo Processing file: %%~nxf
                rem 根据LOCAL_IP_PARTS数量进行for循环遍历
                for /l %%n in (1,1,!LOCAL_IP_PARTS!) do (
                    rem 根据SERV_IP_PARTS数量进行for循环遍历
                    for /l %%m in (1,1,!SERV_IP_PARTS!) do (
                        rem 打印当前遍历的IP对
                        call set "local_ip=%%LOCAL_IP_PART_%%n%%"
                        call set "serv_ip=%%SERV_IP_PART_%%m%%"
                        call :check_ip_protocol_match "!local_ip!" "!serv_ip!" protocol_match
                        @REM echo target file: %%f, IP Pair: !local_ip!---!serv_ip!, Protocol Match: !protocol_match!
                        if "!protocol_match!"=="true" (
                            call :process_cap_file "%%f" "!local_ip!" "!serv_ip!" "!CAPED_FILE_DIR!" "!num!"
                            set /a num+=1
                        )
                    )
                )
            )

            rem capFile's operation is finished, now start Merge Operation
            rem first step: make a dir for merged files
            set "MERGED_FILE_DIR=!LIB_ADD_VALUE!\merged_files"
            if not exist "!MERGED_FILE_DIR!" mkdir "!MERGED_FILE_DIR!"
            rem next step: use the python script to merge the files
            python "merge_test_withFilter.py" "!CAPED_FILE_DIR!" "!MERGED_FILE_DIR!"

            rem Merge Operation is finished, now start extract Features Operation
            rem first step: make a dir for extracted features
            set "EXTRACTED_FEATURES_DIR=!LIB_ADD_VALUE!\extracted_features"
            if not exist "!EXTRACTED_FEATURES_DIR!" mkdir "!EXTRACTED_FEATURES_DIR!"
            rem next step: use the python script to extract features from merged files
            rem our first script is extract_UDP_features.py
            python "extract_UDP_features.py" "!MERGED_FILE_DIR!" "!EXTRACTED_FEATURES_DIR!"
            rem our next script is extract_Modem_features.py
            rem wait for a moment

            rem the final step of extract Features Operation, is merge all data into a single file for clean Operation
            python "combine_features.py" "!EXTRACTED_FEATURES_DIR!"

            rem Extract Features Operation is finished, now start Clean Operation
            rem first step: make a dir for cleaned data
            set "CLEANED_DATA_DIR=!LIB_ADD_VALUE!\cleaned_data"
            if not exist "!CLEANED_DATA_DIR!" mkdir "!CLEANED_DATA_DIR!"
            rem next step: use the python script to clean the data
            python "clean_data_operation.py" "!EXTRACTED_FEATURES_DIR!" "!CLEANED_DATA_DIR!" "!START_TIME_VALUE!" "!END_TIME_VALUE!" "!ID_VALUE!"

            rem 输出2个空行，便于阅读
            echo.
            echo.
            
        )
        
        if "!vision!"=="true" (
            rem after DataPreProcessing, we can visualling the data
            echo start vision Operation
            python "interval_vision.py" "!CSV_PATH!"
        )

    ) else (
        echo Specified CSV file: !CSV_PATH!, does not exist
    )
    
) else (
    echo 未能获取CSV文件路径，脚本执行可能失败
    exit /b 1
)



endlocal
exit /b 0

rem **********************************************************************************
rem 函数：统计CSV文件行数
rem 参数：
rem   %1 - CSV文件路径
rem   %2 - 用于存储结果的变量名
rem 返回值：通过%2变量返回文件行数
rem **********************************************************************************
:CountCSVLines
    set "CSV_FILE=%~1"
    set "RESULT_VAR=%~2"
    set "LINE_COUNT=0"
    
    rem 检查文件是否存在
    if not exist "%CSV_FILE%" (
        set "!RESULT_VAR!=0"
        echo Function CountCSVLines failed to find specified CSV file: %CSV_FILE%
        exit /b 0
    )
    
    rem 统计文件行数
    for /f "usebackq tokens=* delims=" %%a in ("%CSV_FILE%") do (
        set /a LINE_COUNT+=1
    )
    
    rem 设置结果变量
    set "!RESULT_VAR!=!LINE_COUNT!"
    exit /b 0

rem **********************************************************************************
rem 函数：读取CSV文件指定行
rem 参数：
rem   %1 - CSV文件路径
rem   %2 - 行号（从1开始）
rem   %3 - 用于存储结果的变量名
rem 返回值：通过%3变量返回整行内容
rem **********************************************************************************
:ReadCSVLine
    set "CSV_FILE=%~1"
    set "ROW_NUMBER=%~2"
    set "RESULT_VAR=%~3"
    set "CURRENT_ROW=0"
    
    for /f "usebackq delims=" %%a in ("%CSV_FILE%") do (
        set /a CURRENT_ROW+=1
        if !CURRENT_ROW! EQU %ROW_NUMBER% (
            set "!RESULT_VAR!=%%a"
            exit /b 0
        )
    )
    
    rem 如果没有找到指定行，返回空
    set "!RESULT_VAR!="
    exit /b 0

rem **********************************************************************************
rem 函数：读取CSV文件指定单元格
rem 参数：
rem   %1 - CSV文件路径
rem   %2 - 行号（从1开始）
rem   %3 - 列号（从1开始）
rem   %4 - 用于存储结果的变量名
rem 返回值：通过%4变量返回单元格值
rem **********************************************************************************
:ReadCSVCell
    set "CSV_FILE=%~1"
    set "ROW_NUMBER=%~2"
    set "COLUMN_NUMBER=%~3"
    set "RESULT_VAR=%~4"
    set "CELL_VALUE="
    
    rem 验证参数
    if not exist "%CSV_FILE%" (
        echo ReadCSVCell error: File does not exist %CSV_FILE%
        set "!RESULT_VAR!="
        exit /b 1
    )
    
    if %ROW_NUMBER% lss 1 (
        echo ReadCSVCell error: Row number must be greater than 0
        set "!RESULT_VAR!="
        exit /b 1
    )
    
    if %COLUMN_NUMBER% lss 1 (
        echo ReadCSVCell error: Column number must be greater than 0
        set "!RESULT_VAR!="
        exit /b 1
    )
    
    rem 直接在函数内部实现读取行的逻辑
    set "CURRENT_ROW=0"
    set "FOUND_ROW=false"
    
    for /f "usebackq tokens=* delims=" %%a in ("%CSV_FILE%") do (
        set /a CURRENT_ROW+=1
        if !CURRENT_ROW! EQU %ROW_NUMBER% (
            rem 找到了目标行，保存行内容
            set "TARGET_LINE=%%a"
            set "FOUND_ROW=true"
            goto :process_target_row
        )
    )
    
    :process_target_row
    if "!FOUND_ROW!"=="false" (
        echo ReadCSVCell error: Row %ROW_NUMBER% not found
        set "!RESULT_VAR!="
        exit /b 1
    )
    
    rem 使用更简单可靠的方法处理CSV列提取
    echo !TARGET_LINE! > temp_line.csv
    
    rem 使用for /f来提取指定列
    set "CELL_VALUE="
    for /f "tokens=%COLUMN_NUMBER% delims=," %%b in (temp_line.csv) do (
        set "CELL_VALUE=%%b"
    )
    
    rem 清理临时文件
    if exist temp_line.csv del temp_line.csv
    
    rem 去除引号
    if defined CELL_VALUE set "CELL_VALUE=!CELL_VALUE:"=!"
    
    rem 设置结果变量
    set "!RESULT_VAR!=!CELL_VALUE!"
    exit /b 0

rem **********************************************************************************
rem 函数：分割LOCAL_IP地址字符串
rem 参数：
rem   %1 - 要分割的字符串
rem   %2 - 用于存储部分数量的变量名
rem   %3 - 用于存储各部分的变量前缀
rem 返回值：通过%2和%3*变量返回分割结果
rem **********************************************************************************
:split_local_ip
    set "temp=%~1"
    set "part_count_var=%~2"
    set "part_var_prefix=%~3"
    set /a part_count=0
    
    :split_local_loop
    for /f "tokens=1* delims=+" %%a in ("!temp!") do (
        set /a part_count+=1
        set "!part_var_prefix!!part_count!=%%a"
        set "temp=%%b"
    )
    
    rem 检查是否还有剩余部分需要处理
    if not "!temp!"=="" (
        goto :split_local_loop
    )
    
    rem 设置部分数量
    set "!part_count_var!=!part_count!"
    exit /b 0

rem **********************************************************************************
rem 函数：处理cap文件
rem 参数：
rem   %1 - cap文件路径
rem   %2 - 本地IP地址
rem   %3 - 服务端IP地址
rem 返回值：无
rem **********************************************************************************
:process_cap_file
    set "cap_file=%~1"
    set "local_ip=%~2"
    set "serv_ip=%~3"
    set "output_dir=%~4"
    set "num=%~5"
    
    rem 检查cap文件是否存在
    if not exist "!cap_file!" (
        echo Error: CAP file not found: !cap_file!
        exit /b 1
    )
    rem 检查输出目录是否存在
    if not exist "!output_dir!" (
        echo Error: Output directory not found: !output_dir!
        exit /b 1
    )
    
    rem 提取cap文件名（不包含后缀）
    for %%f in ("!cap_file!") do (
        set "cap_filename=%%~nf"
    )
    
    rem 检查local_ip和serv_ip的IP协议类型并确定指令模式
    set "ip_Format_Local=ipv4"
    set "ip_Format_Serv=ipv4"
    set "command_mode="

    rem 判断local_ip是否为IPv6
    if "!local_ip!"=="*" (
        set "ip_Format_Local=any"
    ) else (
        rem 检查是否包含IPv6特征的冒号
        echo !local_ip! | findstr /r ":" >nul
        if !errorlevel! equ 0 set "ip_Format_Local=ipv6"
    )
    
    rem 判断serv_ip是否为IPv6
    if "!serv_ip!"=="*" (
        set "ip_Format_Serv=any"
    ) else (
        rem 检查是否包含IPv6特征的冒号
        echo !serv_ip! | findstr /r ":" >nul
        if !errorlevel! equ 0 set "ip_Format_Serv=ipv6"
    )

    rem 根据local和serv的IP类型确定指令模式
    rem 规则1: 如果local是ipv6格式，serv是ipv6格式或者*，则指令模式为ipv6
    if "!ip_Format_Local!"=="ipv6" (
        set "command_mode=ipv6"
    )
    rem 规则2: 如果local是ipv4格式，serv是ipv4格式或者*，则指令模式为ipv4
    if "!ip_Format_Local!"=="ipv4" (
        set "command_mode=ipv4"
    )
    rem 规则3: 如果local是*，serv是ipv6格式，则指令模式为ipv6
    if "!ip_Format_Local!"=="any" if "!ip_Format_Serv!"=="ipv6" (
        set "command_mode=ipv6"
    )
    rem 规则4: 如果local是*，serv是ipv4格式，则指令模式为ipv4
    if "!ip_Format_Local!"=="any" if "!ip_Format_Serv!"=="ipv4" (
        set "command_mode=ipv4"
    )

    @REM rem 规则5: 如果local和serv都是*，则报错返回
    @REM if "!ip_Format_Local!"=="any" if "!ip_Format_Serv!"=="any" (
    @REM     echo Error: Both local_ip and serv_ip cannot be wildcard (*) at the same time
    @REM     exit /b 1
    @REM )

    rem 根据指令模式执行相应操作
    if "!command_mode!"=="ipv6" (
        rem 这里是IPv6处理逻辑
        @REM echo Command mode: IPv6
        rem TODO: 开始IPv6版本的Tshark指令操作
        if "!ip_Format_Local!"=="ipv6" if "!ip_Format_Serv!"=="ipv6" (
            tshark -r "!cap_file!" -Y "udp and (ipv6.src == !local_ip!) and (ipv6.dst == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip6.src -e ip6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_send_!num!.csv"
            tshark -r "!cap_file!" -Y "udp and (ipv6.src == !serv_ip!) and (ipv6.dst == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip6.src -e ip6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_recv_!num!.csv"
        ) else if "!ip_Format_Local!"=="ipv6" if "!ip_Format_Serv!"=="any" (
            tshark -r "!cap_file!" -Y "udp and (ipv6.src == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ipv6.src -e ipv6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_send_!num!.csv"
            tshark -r "!cap_file!" -Y "udp and (ipv6.dst == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ipv6.src -e ipv6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_recv_!num!.csv"
        ) else if "!ip_Format_Local!"=="any" if "!ip_Format_Serv!"=="ipv6" (
            tshark -r "!cap_file!" -Y "udp and (ipv6.dst == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ipv6.src -e ipv6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_send_!num!.csv"
            tshark -r "!cap_file!" -Y "udp and (ipv6.src == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ipv6.src -e ipv6.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv6_recv_!num!.csv"
        )
    ) else if "!command_mode!"=="ipv4" (
        rem 这里是IPv4处理逻辑
        @REM echo Command mode: IPv4
        rem TODO: 开始IPv4版本的Tshark指令操作
        if "!ip_Format_Local!"=="ipv4" if "!ip_Format_Serv!"=="ipv4" (
            tshark -r "!cap_file!" -Y "udp and (ip.src == !local_ip!) and (ip.dst == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_send_!num!.csv"    
            tshark -r "!cap_file!" -Y "udp and (ip.src == !serv_ip!) and (ip.dst == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_recv_!num!.csv"
        ) else if "!ip_Format_Local!"=="ipv4" if "!ip_Format_Serv!"=="any" (
            tshark -r "!cap_file!" -Y "udp and (ip.src == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_send_!num!.csv"
            tshark -r "!cap_file!" -Y "udp and (ip.dst == !local_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_recv_!num!.csv"
        ) else if "!ip_Format_Local!"=="any" if "!ip_Format_Serv!"=="ipv4" (
            tshark -r "!cap_file!" -Y "udp and (ip.dst == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_send_!num!.csv"   
            tshark -r "!cap_file!" -Y "udp and (ip.src == !serv_ip!)" -T fields -E header=y -E separator=, -E quote=d -e frame.time_epoch -e ip.src -e ip.dst -e frame.protocols -e frame.len -e _ws.col.Info > "!output_dir!\!cap_filename!_IPv4_recv_!num!.csv"   
        )
    )

    @REM rem 记录处理完成
    @REM echo Processing completed for !cap_file!
    
    exit /b 0

rem **********************************************************************************
rem 函数：检查两个IP地址是否属于同一协议类型
rem 参数：
rem   %1 - 第一个IP地址
rem   %2 - 第二个IP地址
rem   %3 - 用于存储结果的变量名（true或false）
rem 返回值：通过%3变量返回结果
rem **********************************************************************************
:check_ip_protocol_match
    set "ip1=%~1"
    set "ip2=%~2"
    set "result_var=%~3"
    
    rem 默认设置为false
    set "!result_var!=false"
    
    rem 检查是否有通配符
    if "!ip1!"=="*" (set "!result_var!=true" & exit /b 0)
    if "!ip2!"=="*" (set "!result_var!=true" & exit /b 0)
    
    rem 检查IP1类型 - IPv6包含冒号
    echo !ip1! | findstr /r "\[" >nul
    if !errorlevel! equ 0 (
        set "ip1_type=IPv6"
    ) else (
        echo !ip1! | findstr /r ":" >nul
        if !errorlevel! equ 0 (
            set "ip1_type=IPv6"
        ) else (
            set "ip1_type=IPv4"
        )
    )
    
    rem 检查IP2类型 - IPv6包含冒号
    echo !ip2! | findstr /r "\[" >nul
    if !errorlevel! equ 0 (
        set "ip2_type=IPv6"
    ) else (
        echo !ip2! | findstr /r ":" >nul
        if !errorlevel! equ 0 (
            set "ip2_type=IPv6"
        ) else (
            set "ip2_type=IPv4"
        )
    )
    
    rem 比较类型
    if "!ip1_type!"=="!ip2_type!" set "!result_var!=true"
    exit /b 0

rem **********************************************************************************
rem 函数：分割SERV_IP地址字符串
rem 参数：
rem   %1 - 要分割的字符串
rem   %2 - 用于存储部分数量的变量名
rem   %3 - 用于存储各部分的变量前缀
rem 返回值：通过%2和%3*变量返回分割结果
rem **********************************************************************************
:split_serv_ip
    set "temp=%~1"
    set "part_count_var=%~2"
    set "part_var_prefix=%~3"
    set /a part_count=0
    
    :split_serv_loop
    for /f "tokens=1* delims=+" %%a in ("!temp!") do (
        set /a part_count+=1
        set "!part_var_prefix!!part_count!=%%a"
        set "temp=%%b"
    )
    
    rem 检查是否还有剩余部分需要处理
    if not "!temp!"=="" (
        goto :split_serv_loop
    )
    
    rem 设置部分数量
    set "!part_count_var!=!part_count!"
    exit /b 0
