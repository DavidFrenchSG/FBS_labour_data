library(tidyverse)
library(haven)
# library(data.table)
# library(ade4)
# library(writexl)

FBS_directory_path <- '//s0177a/sasdata1/ags/fas'

datayear <- 2021
sampyear <- 2021


import_sas <-function(directory_path, datayear, suffix) {
  filename <- paste0("/so_y", datayear, suffix,".sas7bdat")
  dataset <-tryCatch(
    {
      dataset <-read_sas(filename)
    },
    error = function(e)
    {
      return(read_sas(paste0(FBS_directory_path,filename)))
    }
  )
  names(dataset) <- tolower(names(dataset))
  for (var in colnames(dataset)) {
    attr(dataset[[deparse(as.name(var))]],"format.sas")=NULL
  }
  return(dataset)
}
labour_data <- import_sas(FBS_directory_path, datayear, "_alb")
fa_data <- import_sas(FBS_directory_path, datayear, "_fa")
avf_data <- import_sas(FBS_directory_path, datayear, "_avf")
dsec1_data <- import_sas(FBS_directory_path, datayear, "_dsec1")
dsec2_data <- import_sas(FBS_directory_path, datayear, "_dsec2")
ant_data <- import_sas(FBS_directory_path, datayear, "_ant")

labour_data_process <- labour_data %>% 
  mutate(year=fa_id%%10000) %>% 
  filter(year==sampyear) %>% 
  mutate(reg_labour_number=case_when(
    lb_code=="WHRO"~albnump)) %>% 
  mutate(reg_labour_hours=case_when(
    lb_code=="WHRO"~albtimew)) %>% 
  mutate(cas_labour_number=case_when(
    lb_code=="WHCA"~albnump)) %>% 
  mutate(cas_labour_hours=case_when(
    lb_code=="WHCA"~albtimew))

labour_data_summary <- labour_data_process %>% 
  group_by(fa_id) %>% 
  summarise(
    reg_labour_number=sum(reg_labour_number,na.rm=T),
    reg_labour_hours=sum(reg_labour_hours,na.rm=T),
    cas_labour_number=sum(cas_labour_number,na.rm=T),
    cas_labour_hours=sum(cas_labour_hours,na.rm=T)
  )


fa_data_process <- fa_data %>% 
  filter(fa_id %% 10000 == sampyear) %>% 
  select(fa_id, i_lab=fa_ilab, fa_labc) 

avf_data_process <- avf_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(vfc_code=="XVCL") %>% 
  select(fa_id, i_clab=avfcost)

dsec1_env_lab <- dsec1_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(ds1acode%in%c('10200',
                       '10202',
                       '10203',
                       '10204',
                       '10205',
                       '10206',
                       '10207',
                       '10208',
                       '10209',
                       '10210',
                       '10212',
                       '10213')) %>% 
  filter(ds1lc!=0) %>% 
  group_by(fa_id) %>% 
  summarise(ecclab=sum(ds1lc))

dsec1_con_lab <- dsec1_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(ds1acode==11102) %>%  
  filter(ds1lc!=0) %>% 
  select(fa_id, ds1_conlab=ds1lc)
  
dsec1_data_process <- dsec1_con_lab %>% 
  full_join(dsec1_env_lab,by="fa_id")

dsec2_data_process <- dsec2_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(ds2acode==21300) %>%  
  filter(ds2plc!=0) %>% 
  group_by(fa_id) %>% 
  summarise(ds_paidl=sum(ds2plc))

ant_data_process <- ant_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(nt_code %in% c("XNIP","XNIU")) %>% 
  group_by(fa_id,nt_code) %>% 
  summarise(cost=sum(antcost)) %>% 
  pivot_wider(id_cols=fa_id, names_from = nt_code, values_from=cost) %>% 
  rename(i_pdlab = XNIP, i_uflab = XNIU)
  
  

merged_data <- list(fa_data_process, labour_data_summary, dsec1_data_process, dsec2_data_process, ant_data_process, avf_data_process) %>%  
  reduce(full_join, by="fa_id")
merged_data[is.na(merged_data)]=0 
merged_data <- merged_data %>% 
  mutate(f_rlabour=i_lab-(i_pdlab+i_uflab+ ds1_conlab + ecclab + ds_paidl + i_clab)) %>% 
  select(fa_id, reg_labour_number, reg_labour_hours, reg_labour_wages = f_rlabour, cas_labour_number, cas_labour_hours, cas_labour_wages = i_clab) %>% 
  mutate(reg_labour_hourly_wage = reg_labour_wages/reg_labour_hours,cas_labour_hourly_wage = cas_labour_wages/cas_labour_hours)

