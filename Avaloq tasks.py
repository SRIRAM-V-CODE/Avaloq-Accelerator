import pandas as pd
import shutil, os


def print_proc_columns(sheet,f):
    sheet_det=excel_file.parse(sheet)
    cols=sheet_det.columns
    for i in range(len(sheet_det[cols[0]])):
        if not i:f.write(f"   {sheet_det[cols[0]][i]}"+" "*(30-len(sheet_det[cols[0]][i]))+f"{sheet_det[cols[1]][i]}"+" "*(30-len(sheet_det[cols[1]][i]))+":= null\n")
        else:f.write(f"  ,{sheet_det[cols[0]][i]}"+" "*(30-len(sheet_det[cols[0]][i]))+f"{sheet_det[cols[1]][i]}"+" "*(30-len(sheet_det[cols[1]][i]))+":= null\n")
def procedure(row):
    f=open(f"{all_file['value'][row]}.{tags[row]}.OUTPUT.txt",'w')
    proc = excel_file.parse(tags[row])
    f.write(f"\ncreate or replace procedure k.{all_file['value'][row]}\n")
    print_proc_columns(tags[row],f)
    f.write(")\n")
    f.write("is\n")
    cols=[val.strip().lower() for val in proc.columns]
    if "constant" in cols:
        pos=cols.index("constant")
        typ=cols.index("constant type")
        for i in range(len(proc.columns[pos])):
            if str(proc[proc.columns[pos]][i])!="nan":f.write(f"  {proc[proc.columns[pos]][i]}"+" "*(30-len(proc[proc.columns[pos]][i]))+f"constant {proc[proc.columns[typ]][i]}"+" "*(30-len(proc[proc.columns[typ]][i]))+":='NULL';\n") 
            
    f.write("\n")
    f.write("  l_dtm\t\t\tlib_co_sel.extn_rec;\n")
    f.write("  l_sql\t\t\tlib_co_sel.extn_rec;\n")
    f.write("  l_task_exec\t\ttask_exec#.t_exec;\n")
    f.write("  l_file_upl_id\t\tpls_integer := i_file_upl_id;\n")
    f.write("  l_err_txt\t\tvarchar2(1000);\n")
    f.write("  l_file_name\t\tvarchar2(200);\n")
    f.write("\n")
    f.write("  "+'-'*76+"\n")
    f.write("  -- DATAMART GENERATION\n")
    f.write("  "+'-'*76+"\n")
    f.write("  procedure gen_dtm\n")
    f.write("  is\n")
    f.write("    l_sql\tsql#.t_sql;\n")
    f.write("  begin\n")
    f.write("    l_sql := sql#.new(\n")
    f.write("      i_bind_mode     => sql#.c_bind_mode#sql_var\n")
    f.write("     ,i_task_def_id   => task_exec#.g_task_def_id\n")
    f.write("     ,i_task_templ_id => task_exec#.g_task_templ_id\n")
    f.write("    );\n")
    f.write("    sql#.sql#set(\n")
    f.write("      i_sql    => l_sql\n")
    f.write(f"     ,i_select => ' /*+ '||sql#.task_templ_hint||' {all_file['value'][tag.index('task def')]}.gen_dtm */'\n")
    f.write("\t|| mba$file_upl#.log_file_upl_id || ' log_file_upl_id ,dt.* '\n")
    f.write(f"\t,i_from     =>'{all_file['value'][tag.index('task def')]} dt');\n")
    f.write("    sql#.sql#get_extn_rec(l_sql,x_extn_rec => l_dtm);\n")
    f.write("    sql#.sql#remv(l_sql);\n")
    f.write("\n")
    f.write("  exception\n")
    f.write("    when others then\n")  
    f.write(f"      raise_fa_err('{all_file['value'][tag.index('task def')]}.gen_dtm');\n")
    f.write("  end gen_dtm;\n")
    f.write("\n")
    f.write("-"*76+"\n")
    f.write("-- MAIN\n")
    f.write("-"*76+"\n")
    f.write("begin\n")
    f.write("   l_file_name := i_file_name || to_char(i_ref_date, s#mba$date.c_fmt) || c_file_format;\n")
    f.write("\n")
    f.write("   begin\n")
    f.write("     if i_file_name is not null then\n")
    f.write("       mba$file_upl#.file_upl#upl_srv(\n")
    f.write("         o_file_upl_id      => l_file_upl_id\n")
    f.write(f"        ,i_file_upl_type_id => {proc['constant'][1]}\n")
    f.write("        ,i_file_name        => l_file_name\n")
    f.write("        ,i_dir_alias        => i_dir_alias\n")
    f.write("       );\n")
    f.write("     end if;\n")
    f.write("     mba$file_upl#.file_upl#upl(\n")
    f.write("       i_file_upl_id      => l_file_upl_id\n")
    f.write(f"      ,i_file_upl_type_id => {proc['constant'][1]}\n")
    f.write("     );\n")
    f.write("   exception\n")
    f.write("     when others then\n")
    f.write("       l_err_txt := 'Error while processing upload file';\n")
    f.write("       s#mba$log.warn(i_ctx => l_err_txt);\n")
    f.write("       if session#.session#is_intf then\n")
    f.write("         ui#.add_info_msg(l_err_txt);\n")
    f.write("       end if;\n")
    f.write("       return;\n")
    f.write("   end;\n")
    f.write("\n")
    f.write("   ---- GENERATE LAYOUT ----\n")
    f.write("  gen_dtm;\n")
    f.write("\n")
    f.write("  layout#.do (\n")
    f.write("    i_task_id\t=> i_out_job_id\n")
    f.write("   ,i_lang_id\t=> i_lang_id\n")
    f.write("   ,i_dtm\t=> l_dtm\n")
    f.write("   ,i_layout_id\t=> i_layout_id\n")
    f.write("  );\n")
    f.write("\n")
    f.write("  mba$file_upl#.file_upl#cmpl(i_file_upl_id => l_file_upl_id);\n")
    f.write("\n")
    f.write("exception\n")
    f.write("  when others then\n")
    f.write("    lock#.nlock#rls(mba$file_upl#.file_upl#lock_hdl(c_mmkt_upl_type_id));\n")
    f.write("    raise_fa_err (\n")
    f.write("      'ubp$task_mmkt_upl('\n")
    f.write("      || i_file_upl_id \t|| ','\n")
    f.write("      || i_file_name\t|| ','\n")
    f.write("      || i_dir_alias\t|| ','\n")
    f.write("      || i_layout_id\t|| ','\n")
    f.write("      || i_opn_wfa_id\t|| ','\n")
    f.write("      || i_store_wfa_id\t|| ','\n")
    f.write("      || i_lang_id\t|| ','\n")
    f.write("      || i_out_job_id\t|| ')'\n")
    f.write("    );\n")
    f.write("end ubp$task_mmkt_upl;\n")
    f.close()



def taskDefinition(row):
        task=excel_file.parse(tags[row])
        col=task.columns

        k,i=0,0
        f=open(f"{all_file['value'][row]}.{tags[row]}.OUTPUT.txt",'w')
        f.write("[task definition 1.0]\n\n")
        f.write(f"task definition {all_file['value'][row]} ()\n\n")
        f.write("report (online batch)\n\n")
        f.write("  naming\n")
        f.write('   dfltlang\t\t"" \n\n')
        f.write(' '*2+'-'*76+'\n')
        f.write('    -- TASK ATTRIBUTE\n')
        f.write(' '*2+'-'*76+'\n\n')
        f.write('descn    \t\t""\n')
        f.write('hira     \t\t""\n')
        f.write('coverpage\t\tfalse\n')
        f.write('lock_templates\t\tfalse\n')
        f.write(f'datamart\t\t"{all_file["value"][tag.index("ddic")]}" (no_override)\n')
        f.write('lock_templates\t\tfalse\n')
        f.write('context  \t\tnull (no_override)\n')
        f.write('single_login\t\ttrue (no_override)\n\n\n')
        f.write(f'procedure {all_file["value"][tag.index("procedure")]}\n\n')
        f.write(' '*2+'-'*76+'\n')
        f.write('    -- PARAMETER\n')
        f.write(' '*2+'-'*60+'\n')
        f.write("\ti_file_upl_id\tfile\n")
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        k+=1
        f.write("\ti_file_name\tfile\n")
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        k+=1
        f.write("\ti_dir_alias\tfile\n")
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        k+=1
        f.write(' '*2+'-'*60+'\n')
        f.write('    -- LAYOUT\n')
        f.write(' '*2+'-'*60+'\n')
        f.write("  separator\n")
        k=0
        i+=1
        f.write('\tstyle\t\t"main_sep"\n')
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        k+=1
        f.write('\ti_layout_id\tcode obj_task_layout_v \n')
        f.write(f'\twhere\t\t"task_def = \'{all_file["value"][row]}\'" \n')
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        k+=1
        f.write('\tstyle\t\t"main_sep"\n')
        f.write(f'\tlabel\t\t"LABEL.{task[col[i]][k]}"\n\n')
        k=0
        i+=1
        f.write(' '*2+'-'*60+'\n')
        f.write('    -- PROCESSING OPTION\n')
        f.write(' '*2+'-'*60+'\n\n')
        f.write('    separator\n')
        f.write('      style\t\t"main_sep"\n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')

        f.write('    i_do_gen\t\tboolean\n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n')
        k+=1
        f.write('      descn\t\t"Generate MMKT Report"\n\n')
        f.write(' '*2+'-'*60+'\n')

        f.write('    i_opn_wfa_id\tcode wfc_action\n')
        f.write('      where\t\t"meta_typ_id = def_meta_typ.mmkt" \n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        k+=1

        f.write('    i_store_wfa_id\tcode wfc_action\n')
        f.write('      where\t\t"meta_typ_id = def_meta_typ.mmkt"\n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n\n')
        k=0
        
        f.write(' ' * 2 + '-' * 60 + '\n')
        f.write('    -- DATE\n')
        f.write(' ' * 2 + '-' * 60 + '\n\n')

        f.write('    separator\n')
        f.write('      style\t\t"sub_sep"\n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')

        f.write('    i_ref_date\t\tdate\n')
        f.write('      mandatory\n')
        f.write(f'      label\t\t"LABEL.{task[col[i]][k]}"\n\n')
        f.write(' '*2+'-'*60+'\n')
        f.write('end task definition\n')

        f.close()

def write_script_header(f,row):
    f.write("[script 1.0]\n\n")
    f.write(f"script package {all_file['value'][row].lower()}\n")
    f.write("is\n\n")


def write_script_imports(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  -- IMPORT\n")
    f.write("  "+"-"*60+"\n")
    f.write("  ---- AVALOQ ----\n")
    f.write("  import\terr;\n\n")
    f.write("  ---- MBA ----\n")
    f.write("  import\t\tmba$log;\n")
    f.write("  import\t\tmba$mem_doc;\n")
    f.write("  import\t\tmba$file_upl;\n")
    f.write("  import\t\tmba$mem_doc_mmkt;\n\n")


def write_script_constants(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  -- CONSTANTS\n")
    f.write("  "+"-"*60+"\n")
    f.write("  c_mmkt_std_form_id\t\t\t\t constant number := lookup.code('meta_typ', 'mmkt#mba$std', 'intl_id');\n\n")


def write_script_variables(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  -- CONSTANTS\n")
    f.write("  "+"-"*60+"\n")
    f.write("  ---- CURRENT DOC ----\n")
    f.write("  b_doc\t\t\t\t\t\t\t mem_doc_mmkt;\n")
    f.write("  ---- LAYOUT PROCESSING ----\n")
    f.write("  b_lp_err"+" "*(30-len("b_lp_err"))+" text;\n")
    f.write("  b_lp_info"+" "*(30-len("b_lp_info"))+" text;\n")
    f.write("  b_lp_err_log"+" "*(30-len("b_lp_err_log"))+" number;\n")
    f.write("  b_lp_doc"+" "*(30-len("b_lp_doc"))+" doc_clt;\n")
    f.write("  b_lp_seq_nr"+" "*(30-len("b_lp_seq_nr"))+" number;\n")
    f.write("  b_lp_clt_bp_id"+" "*(30-len("b_lp_clt_bp_id"))+" text;\n")
    f.write("  b_lp_qty"+" "*(30-len("b_lp_qty"))+" text;\n")
    f.write("  b_lp_asset_key"+" "*(30-len("b_lp_asset_key"))+" text;\n")
    f.write("  b_lp_period"+" "*(30-len("b_lp_period"))+" text;\n")
    f.write("  b_lp_mkt_rate"+" "*(30-len("b_lp_mkt_rate"))+" text;\n")
    f.write("  b_lp_pay_period"+" "*(30-len("b_lp_pay_period"))+" text;\n\n")


def write_script_getter_functions(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  -- GETTER\n")
    f.write("--@Format-Off\n")
    f.write("  "+"-"*60+"\n")
    f.write("  function lp_err\t\t\t\t\t return text\t is begin return b_lp_err;\t\t\t\t\t end lp_err;\n\n")
    f.write("  "+"-"*60+"\n")
    f.write("  function doc\t\t\t\t\t\t return number is begin return mba$mem_doc.doc#doc(b_doc);\t end doc;\n\n")
    f.write("  "+"-"*60+"\n")
    f.write("  function lp_doc\t\t\t\t\t return number\t is begin return b_lp_doc;\t\t\t\t\t end lp_doc;\n\n")
    f.write("  "+"-"*60+"\n")
    f.write("  function lp_info\t\t\t\t\t return text\t is begin return b_lp_info;\t\t\t\t\t end lp_info;\n\n")
    f.write("  "+"-"*60+"\n")
    f.write("  function lp_err_log_id\t\t\t return number\t is begin return b_lp_err_log;\t\t\t\t end lp_err_log_id;\n\n")


def write_script_reset_procedure(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  "+"-"*60+"\n")
    f.write("  -- RESET\n")
    f.write("  "+"-"*60+"\n")
    f.write("  procedure lp#prc_row(\n")
    script=excel_file.parse(tags[row])
    for val in range(len(script['Procedure column'])):
        f.write(f"\t  {script['Procedure column'][val]}"+" "*(30-len(script['Procedure column'][val]))+f" {script['Procedure type'][val]}\n")
    f.write("  )\n")
    f.write("  is\n")
    f.write("  begin\n")
    f.write("\t  ---- RESET ----\n")
    f.write("\t  b_doc\t\t\t\t\t := null;\n")
    f.write("\t  b_lp_err\t\t\t\t := null;\n")
    f.write("\t  b_lp_err_log\t\t\t\t := null;\n")
    f.write("\t  b_lp_doc\t\t\t\t := null;\n")
    pos=-1
    for i in range(len(script['Procedure column'])):
        if script['Procedure column'][i].strip()=="i_file_name":
            pos=i
            break
    print(pos)
    if pos>=0:
        for val in range(pos,len(script['Procedure column'])):
            f.write(f"\t  b_lp_{script['Procedure column'][val][2:]}"+" "*(35-len(script['Procedure column'][val][2:])-3)+"  := null;\n")
    f.write("\n\n  ---- COMMON VALIDATION ----\n")
    f.write("  // Write Avaloq Logic below \n\n\n\n\n")
    f.write("    end lp#prc_row;\n")
    f.write(f"  end {all_file['value'][row].lower()};\n")


def script_package(row):
    with open(f"{all_file['value'][row].lower()}.OUTPUT.txt", "w") as f:
        write_script_header(f,row)
        write_script_imports(f,row)
        write_script_constants(f,row)
        write_script_variables(f,row)
        write_script_getter_functions(f,row)
        write_script_reset_procedure(f,row)


def write_report_header(f,row):
    f.write("[report 2.0]\n\n")
    f.write(f"report {all_file['value'][row]}\n")
    f.write("\n")
    f.write("  datamart\n\n")

def write_report_init(f,row):
    f.write("    on init\n")
    f.write("      opn_wfa_id"+" "*(30-len("opn_wfa_id"))+"number"+" "*(24)+"assign"+" "*(24)+"[session.curr_task_exec.param('opn_wfa_id').id_val]\n")
    f.write("      store_wfa_id"+" "*(30-len("store_wfa_id"))+"number"+" "*(24)+"assign"+" "*(24)+"[session.curr_task_exec.param('store_wfa_id').id_val]\n")
    f.write("      do_gen"+" "*(24)+"text"+" "*(26)+"assign"+" "*(24)+"[coalesce(task_exec.param('do_gen').text_val, '-')]   \n")
    f.write("      file_name"+" "*(21)+"text"+" "*(26)+"assign"+" "*(24)+"[file_upl#.file#file_name(task_exec.param('file_upl_id').nr_val)]\n\n")

def write_report_connect(f,row):
    f.write(f"    connect {all_file['value'][tag.index('ddic')]} as dt\t\n\n")

def write_report_split(f,row):
    f.write("    split by\n")
    f.write("      seq_nr                            [dt.seq_nr]\n\n")

def write_report_consolidate(f,row):
    f.write("    consolidate\n")
    f.write("      on seq_nr\n")
    rep=excel_file.parse(tags[row])
    for val in range(len(rep['Type'])):
        f.write(f"\t{rep['Column name'][val]}"+" "*(20-len(rep['Column name'][val]))+f"{rep['Type'][val]}"+" "*(20-len(str(rep['Type'][val])))+f"assign\t\t[dt.{rep['Column name'][val]}]\n")
    f.write(f"\n     end datamart\n")
    f.write(f"\n end report\n")

def repdtm(row):
    with open(f"{all_file['value'][row]}.OUTPUT.txt", "w") as f:
        write_report_header(f,row)
        write_report_init(f,row)
        write_report_connect(f,row)
        write_report_split(f,row)
        write_report_consolidate(f,row)




def write_report_header(f,row):
    f.write("[report 2.0]\n\n")
    f.write(f"report {all_file['value'][row]}\n")

def write_header_section(f,row):
    f.write("  "+"-"*76+"\n")
    f.write("\t-- HEADER\n")
    f.write("  "+"-"*60+"\n")
    f.write("    naming\n")
    f.write("\tdfltlang\t\t\t\t\t'NULL'\n")
    f.write("\n")
    f.write("    user_id\t\t\t\t\t'NULL'\n")
    f.write("\n")

def write_imports(f,row):
    f.write("  "+"-"*76+"\n")
    f.write("-- IMPORTS\n")
    f.write("  "+"-"*60+"\n")
    f.write("---- MBA ----\n")
    f.write("import\t\t\t\t\tubp$mmkt_upl;\n")
    f.write("import\t\t\t\t\tmba$mem_doc;\n")
    f.write("\n")

def write_datamart_section(f,row):
    f.write("  "+"-"*76+"\n")
    f.write("-- DATAMART\n")
    f.write("  "+"-"*60+"\n")
    f.write(f"datamart {dtm_value.lower()}\n")
    f.write("\n")

def write_layout_script_section(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("-- LAYOUT\n")
    f.write("  "+"-"*60+"\n")
    f.write("script layout\n")
    f.write("\n")

def write_on_seq_nr_head(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  on seq_nr head\n")
    f.write("\n")

def write_ubp_script_call(f,row):
    f.write(f"    {all_file['value'][tag.index('script package')].lower()}.#prc_row(\n")
    f.write("       i_log_file_upl"+" "*(16)+"=> seq_nr.log_file_upl_id\n")
    f.write("      ,i_do_gen"+" "*(22)+"=> top.do_gen\n")
    f.write("      ,i_file_name"+" "*(19)+"=> top.file_name\n")
    f.write("      ,i_opn_wfa_id"+" "*(18)+"=> top.opn_wfa_id\n")
    f.write("      ,i_store_wfa_id"+" "*(16)+"=> top.store_wfa_id\n")
    screen=excel_file.parse(tags[row])
    script=excel_file.parse('SCRIPT PACKAGE')
    i=0
    while i<len(script['Procedure column']):
        if script['Procedure column'][i]=='i_file_name':
            i+=1
            break
        i+=1
    pos=i
    for i in range(pos,len(script['Procedure column'])):
        if i==pos:
            f.write(f"      ,{script['Procedure column'][i]}"+" "*(30-len(script['Procedure column'][i]))+f"=> {script['Procedure column'][i][2:]}\n")
        else:
            f.write(f"      ,{script['Procedure column'][i]}"+" "*(30-len(script['Procedure column'][i]))+f"=> {script['Procedure column'][pos][2:].strip()}.{script['Procedure column'][i][2:]}\n")
    f.write("      );\n")
    f.write("\n")
            
def write_commit_reset(f,row):
    f.write("   -- COMMIT TO REDUCE CONCURRENCY\n")
    f.write("     session.commit;\n")
    f.write("\n")
    f.write("   -- RESET PROCESS MEMORY\n")
    f.write("     if mod (seq_nr , mba$mem_doc.c_max_doc_cnt) = 0 then\n")
    f.write("       session.reset;\n")
    f.write("     end if;\n")
    f.write("\n")

def write_report_foot(f,row):
    f.write("  "+"-"*60+"\n")
    f.write("  on report foot\n")
    f.write("     session.commit;\n")
    f.write("     session.reset;\n")
    f.write("\n")
    f.write(" end layout\n")
    f.write("\n")

def write_report_end(f,row):
    f.write("end report\n")

def rep_script(row):
    with open(f"{all_file['value'][row].lower()}.OUTPUT.txt", "w") as f:
        write_report_header(f,row)
        write_header_section(f,row)
        write_imports(f,row)
        write_datamart_section(f,row)
        write_layout_script_section(f,row)
        write_on_seq_nr_head(f,row)
        write_ubp_script_call(f,row)
        write_commit_reset(f,row)
        write_report_foot(f,row)
        write_report_end(f,row)

def task_tamplate_1(row):
    with open(f"{all_file['value'][row].lower()}.OUTPUT.txt", "w") as f:
        f.write("[task template 1.0]\n")
        f.write("\n")
        f.write(f"task template {all_file['value'][row]}\n")
        f.write("\n")
        f.write(f"    task\t\t\t\t\t{all_file['value'][tag.index('task def')].lower()} (no_override)\n")
        f.write("    slice_cnt\t\t\t\t\t1\n")
        f.write('    name\t\t\t\t\t""\n')
        f.write("\n")
        temp1=excel_file.parse(tags[row].capitalize())
        f.write(f"    param i_layout_id\n")
        f.write(f"\tscript:{all_file['value'][tag.index('rep script')].lower()}\t\t\t\n")
        f.write("\n")
                
        
        if len(temp1):
            for i in range(len(temp1)):
                f.write(f"    param {temp1['Parameters'][i]}\n")
                f.write('\t""')
                f.write("\n")
        f.write("end task template\n")

        
def task_tamplate_2(row):
    with open(f"{all_file['value'][row].lower()}.OUTPUT.txt", "w") as f:
        f.write("[task template 1.0]\n")
        f.write("\n")
        f.write(f"task template {all_file['value'][row]}\n")
        f.write("\n")
        f.write(f"    task\t\t\t\t\t{all_file['value'][tag.index('task def')].lower()} (no_override)\n")
        f.write('    name\t\t\t\t\t""\n')
        f.write("\n")
        temp1=excel_file.parse(tags[row].capitalize())
        f.write(f"    param i_layout_id\n")
        f.write(f"\tscreen:{all_file['value'][tag.index('rep script')].lower()}\t\t\t\n")
        f.write("\n")
                
        
        if len(temp1)>0:
            for i in range(len(temp1)):
                f.write(f"    param {temp1['Parameters'][i]}\n")
                f.write('\t""')
                f.write("\n")
        f.write("end task template\n")

def DDIC(row):
    with open(f"{all_file['value'][row].lower()}.{tags[row]}.OUTPUT.txt", "w") as f:
        ddic=excel_file.parse(tags[row])
        f.write("[data dictionary 1.0]\n")
        f.write("\n")
        f.write(f"datamart {all_file['value'][row]}\n")
        for i in range(len(ddic)):
            if ddic['Column Name'][i]=="seq_nr":
                f.write("\n")
                f.write("  "+"-"*60+"\n")
                f.write(f"    {ddic['Column Name'][i]}\t{ddic['Avaloq Data Type'][i]}\n")
                if str(ddic['Remarks'][i])!="nan":
                    f.write(f"\tdescn\t[{ddic['Remarks'][i]}]\n")
                f.write("\tlabel\t\t[Label.]\n")
                f.write(f"\tselect\t\t[dt.{ddic['Column Name'][i]}]\n")
                f.write(f"\talias\t\t[{ddic['Column Name'][i]} ]\n")
            else:
                f.write("\n")
                f.write("  "+"-"*60+"\n")
                f.write(f"    {ddic['Column Name'][i]}\t{ddic['Avaloq Data Type'][i]}\n")
                if str(ddic['Remarks'][i])!="nan":
                    f.write(f"\tdescn\t\t[{ddic['Remarks'][i]}]\n")
                f.write(f"\tselect\t\t[dt.{ddic['Column Name'][i]}]\n")
                f.write(f"\talias\t\t[{ddic['Column Name'][i]}]\n")
        f.write("\n")
        f.write("end datamart\n")
def tab_def(row):
        f=open(f'{all_file["value"][row].lower()}.{tags[row]}.OUTPUT.txt','w')
        f.write('[data dictionary 1.0]\n')
        f.write(f"\ntable definition {all_file['value'][row]}\n\n")
        f.write("    columns\n")
        tab=excel_file.parse(tags[row])
        for ptr in range(len(tab)):
            f.write(f"\t{tab['Column Name'][ptr]}"+" "*(30-len(tab['Column Name'][ptr].strip()))+f"{tab['Data type'][ptr]}\n")
        f.write("\n\n")
        f.write("  organization external\n")
        f.write("    type"+" "*26+"[oracle_loader]\n")
        f.write("    default directory"+" "*(30-len("default directory"))+"\"aaa_db_io\"\n")
        f.write(f"    location"+" "*(30-len("location"))+f"{all_file['value'][row].lower()}.norm\"\n")
        f.write("    reject limit"+" "*(30-len("reject limit"))+"unlimited\n")
        f.write("    access parameters"+" "*(30-len("access parameters"))+"[\n")
        f.write("    records delimited by newline\n")
        f.write(f"\tbadfile"+" "*(23)+f"'{all_file['value'][row].lower()}.bad'\n")
        f.write(f"\tlogfile"+" "*(23)+f"'{all_file['value'][row].lower()}.log'\n")
        f.write(f"\tdiscardfile"+" "*19+f"'{all_file['value'][row].lower()}.dis'\n")
        f.write("\tskip 1\n")
        f.write("\tcharacterset utf8\n")
        f.write("\tfields  terminated by ','\n")
        f.write("\toptionally enclosed by '\"'\n")
        f.write("\tmissing field values are null\n")
        f.write("\treject rows with all null fields\n\t(\n")
        samp=0
        for ptr in range(len(tab['Column Name'])):
            if str(tab['mand'][ptr]).lower()=="yes":
                if not samp:
                    f.write(f"\t {tab['Column Name'][ptr]}\t\t{tab['Avaloq Data Type'][ptr]}\n")
                    samp=1
                else:
                    f.write(f"\t ,{tab['Column Name'][ptr]}\t\t{tab['Avaloq Data Type'][ptr]}\n")

        f.write("\t)\n\nend table definition")











file_=input("Enter excel file name: ").strip()+".xlsx"
excel_file = pd.ExcelFile(file_)
all_file=excel_file.parse(excel_file.sheet_names[0])
tags=all_file['tag'].tolist()
tag=[val.strip().lower() for val in tags]
dtm_value=""
for row in range(len(tags)):
    if "ddic"in tag[row]:
        DDIC(row)
    if "task def"in tag[row]:
        taskDefinition(row)
    if "procedure" in tag[row]:
        procedure(row)
    if "script package" in tag[row]:
        script_package(row)
    if "rep dtm" in tag[row]:
        repdtm(row)
        dtm_value=all_file['value'][row].split('.')
        try:
            dtm_value=dtm_value[1]
        except Exception as e:
            print(e)
    if "rep screen" in tag[row]:
        with open(f"{all_file['value'][row].lower()}.OUTPUT.txt", "w") as f:
            f.write("[report 2.0]\n")
            f.write("\n")
            f.write(f"report {all_file['value'][row].lower()}\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("  -- HEADER\n")
            f.write("    "+"-"*76+"\n")
            f.write("  naming\n")
            f.write("    dfltlang                            'NULL'\n")
            f.write("\n")
            f.write("  user_id                               'NULL'\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("  -- IMPORTS\n")
            f.write("    "+"-"*76+"\n")
            f.write("\n")
            f.write("  ---- UBP ----\n")
            f.write("  import           ubp$mmkt_upl;\n")
            f.write("\n")
            f.write("  ---- MBA ----\n")
            f.write("  import           mba$mem_doc;\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("  -- DATAMART\n")
            f.write("    "+"-"*76+"\n")
            f.write(f"  datamart {dtm_value}\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("  -- LAYOUT\n")
            f.write("    "+"-"*76+"\n")
            f.write("  screen layout\n")
            f.write("    order by                            1\n")
            f.write("    "+"-"*76+"\n")
            f.write("    -- LAYOUT SETTINGS\n")
            f.write("    "+"-"*76+"\n")
            f.write("    title                               'NULL'\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            screen=excel_file.parse(tags[row])
            print(screen)
            f.write("    on column head\n")
            for i in range(len(screen['Column name'])):
                if not i:
                    f.write(f"      column {screen['Column name'][i]} {screen['Column type'][i]}\n")
                    f.write("        align"+" "*25+"right\n")
                    f.write("        label"+" "*25+f"session.text('LABEL.{screen['Label'][i]}');\n")
                    f.write("\n")
                else:
                    f.write(f"      column {screen['Column name'][i]} {screen['Column type'][i]}\n")
                    f.write("        label"+" "*25+f"session.text('LABEL.{screen['Label'][i]}');\n")
                    f.write("\n")
                    
            f.write("    "+"-"*76+"\n")
            f.write("    on report head\n")
            f.write("      if session.is_intf = '+' and top.seq_nr is null then\n")
            f.write("        style 'line';\n")
            f.write("        column seq_nr                   session.text('LABEL.NODATA_FOUND');\n")
            f.write("      end if;\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("    on seq_nr head\n")
            f.write(f"      {all_file['value'][tag.index('script package')].lower()}.lp#prc_row(\n")
            f.write("        i_log_file_upl"+" "*(16)+"=> seq_nr.log_file_upl_id\n")
            f.write("       ,i_do_gen"+" "*(22)+"=> top.do_gen\n")
            f.write("       ,i_file_name"+" "*(19)+"=> top.file_name\n")
            f.write("       ,i_opn_wfa_id"+" "*(18)+"=> top.opn_wfa_id\n")
            f.write("       ,i_store_wfa_id"+" "*(16)+"=> top.store_wfa_id\n")
            script=excel_file.parse('SCRIPT PACKAGE')
            print(script)
            i=0
            while i<len(script['Procedure column']):
                if script['Procedure column'][i]=='i_file_name':
                    i+=1
                    break
                i+=1
            pos=i
            for i in range(pos,len(script['Procedure column'])):
                if i==pos:
                    f.write(f"       ,{script['Procedure column'][i]}"+" "*(30-len(script['Procedure column'][i]))+f"=> {script['Procedure column'][i][2:]}\n")
                else:
                    f.write(f"       ,{script['Procedure column'][i]}"+" "*(30-len(script['Procedure column'][i]))+f"=> {script['Procedure column'][pos][2:].strip()}.{script['Procedure column'][i][2:]}\n")
                    
            f.write("      );\n")
            f.write("\n")
            for i in range(len(screen['Column name'])):
                if not i:
                    f.write(f"      column {screen['Column name'][i]}"+" "*(30-len(screen['Column name'][i]))+f"{screen['Column name'][i]};\n")
                else:
                    f.write(f"      column {screen['Column name'][i]}"+" "*(30-len(screen['Column name'][i]))+f"{screen['Column name'][0]}.{screen['Column name'][i]};\n")
            f.write("      column lp_doc                     ctx 'doc' ubp$mmkt_upl.lp_doc\n")
            f.write("                                        ubp$mmkt_upl.lp_doc;\n")
            f.write("      column lp_err                     ubp$mmkt_upl.lp_err;\n")
            f.write("\n")
            f.write("      -- COMMIT TO REDUCE CONCURRENCY\n")
            f.write("      session.commit;\n")
            f.write("\n")
            f.write("      -- RESET PROCESS MEMORY\n")
            f.write("    if mod (seq_nr, mba$mem_doc.c_max_doc_cnt) = 0 then\n")
            f.write("        session.reset;\n")
            f.write("      end if;\n")
            f.write("\n")
            f.write("    "+"-"*76+"\n")
            f.write("    on report foot\n")
            f.write("      session.commit;\n")
            f.write("      session.reset;\n")
            f.write("\n")
            f.write("  end layout\n")
            f.write("\n")
            f.write("end report\n")

    if "rep script" in tag[row]:
        rep_script(row)
    if "task template 1" in tag[row]:
        task_tamplate_1(row)
    if "task template 2" in tag[row]:
        task_tamplate_2(row)
    if "tab def" in tag[row]:
        tab_def(row)

path = input("Enter Folder Directory:").strip()

if not os.path.exists(path + "\Incoming Files Upload"):
        os.makedirs(path + r"\Incoming Files Upload")
files = os.listdir(path)
for file in files:
    if ".OUTPUT." in file:
            shutil.move(f"{path}\{file}",f"{path}\Incoming Files Upload\{file}")
print("Folder created!!!!!!")







































































