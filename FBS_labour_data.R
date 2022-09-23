library(tidyverse)
library(haven)
# library(data.table)
# library(ade4)
# library(writexl)

FBS_directory_path <- '//s0177a/sasdata1/ags/fas'

datayear <- 2021
sampyear <- 2021

#Get Labour data
labour_data_filename <- paste0("/so_y", datayear, "_alb",".sas7bdat")

labour_data <-tryCatch(
  {
  labour_data <-read_sas(labour_data_filename)
  },
  error = function(e)
  {
    return(read_sas(paste0(FBS_directory_path,labour_data_filename)))
  }
)

names(labour_data) <- tolower(names(labour_data))
for (x in colnames(labour_data)){
  attr(labour_data[[deparse(as.name(x))]],"format.sas")=NULL
}

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
