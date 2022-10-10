library(tidyverse)
library(haven)
library(data.table)
# library(ade4)
library(writexl)

## Enter the year you want data from here. "sampyear" means the actual year data refers to and "datayear" is the year the data was gathered.
## If you want the most recent (provisional) data available, then datayear should equal sampyear.
## For finalised data datayear should be equal to sampyear + 1.
sampyear <- 2021
datayear <- 2021

## FBS threshold variables. The exchange rate is from 2013 (set by standard outputs).
FBS_euro_threshold <- 25000
exchange_rate <- 0.8526
FBS_slr_threshold <- 0.5

## Setting up a lookup table to add farmtypes.
type_numbers <- c(1:9)
type_words <- c("Cereals","General Cropping","Dairy","LFA Sheep","LFA Cattle","LFA Cattle and Sheep","Lowland Livestock","Mixed","All farm types")
type_tab <- data.frame(type_numbers, type_words)

## Location of the FBS SAS server
FBS_directory_path <- '//s0177a/sasdata1/ags/fas'

## A function for reading in and cleaning SAS datasets.
## It first checks to see if the dataset has already been downloaded to the present working directory, and then reads it from the SAS drive if not.
## Cleaning involves converting column names to lower case and stripping attributes (which prevents warnings when joining tables).
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

## Use the above function to read in and clean the various FBS datasets
alb_data <- import_sas(FBS_directory_path, datayear, "_alb")
fa_data <- import_sas(FBS_directory_path, datayear, "_fa")
avf_data <- import_sas(FBS_directory_path, datayear, "_avf")
dsec1_data <- import_sas(FBS_directory_path, datayear, "_dsec1")
dsec2_data <- import_sas(FBS_directory_path, datayear, "_dsec2")
ant_data <- import_sas(FBS_directory_path, datayear, "_ant")

## Separately, get the census data (used for scaling up to national level)
census_data_file <- paste0("june",sampyear-1,".sas7bdat")
census_directory_path <- '//s0177a/sasdata1/ags/census/agscens'
census_data <- tryCatch(
  {
    census_data <- read_sas(census_data_file)
  },
  error = function(e)
  {
    return(read_sas(paste0(census_directory_path,"/",census_data_file)))
  }
)

## Import weights from FAS folder
weights_file <- paste(FBS_directory_path, "/new_weights.sas7bdat", sep='')
weights <- read_sas(weights_file)
names(weights) <- tolower(names(weights))
for (x in colnames(weights)){
  attr(weights[[deparse(as.name(x))]],"format.sas")=NULL
}
## For unweighted figures, uncomment these next two lines.
# weights <- weights %>% 
#   mutate(fbswt=1)

## Process labour data
alb_data_process <- alb_data %>% 
  mutate(year=fa_id%%10000) %>% 
  filter(year==sampyear) %>% 
  mutate(reg_labour_number=case_when(
    lb_code=="WHRO"~albnump)) %>% 
  mutate(reg_labour_hours=case_when(
    lb_code=="WHRO"~albtimew)) %>% 
  mutate(cas_labour_number=case_when(
    lb_code=="WHCA"~albnump)) %>% 
  mutate(cas_labour_hours=case_when(
    lb_code=="WHCA"~albtimew)) %>% 
  group_by(fa_id) %>% 
  summarise(
    reg_labour_number=sum(reg_labour_number,na.rm=T),
    reg_labour_hours=sum(reg_labour_hours,na.rm=T),
    cas_labour_number=sum(cas_labour_number,na.rm=T),
    cas_labour_hours=sum(cas_labour_hours,na.rm=T)
  )

## Process farm account data to get farm type and total labour cost for each farm (i_lab)
fa_data_process <- fa_data %>% 
  filter(fa_id %% 10000 == sampyear) %>% 
  select(fa_id, i_lab=fa_ilab, type) 

## Process VFC expenditure data to get casual labour cost for each farm (i_clab)
avf_data_process <- avf_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(vfc_code=="XVCL") %>% 
  select(fa_id, i_clab=avfcost)

## Process dsec1 data. Done in two stages - labour costs from environmental projects (ecclab) and then labour costs from contracting (conlab)
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
dsec1_conlab <- dsec1_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(ds1acode==11102) %>%  
  filter(ds1lc!=0) %>% 
  select(fa_id, ds1_conlab=ds1lc)
dsec1_data_process <- dsec1_conlab %>% 
  full_join(dsec1_env_lab,by="fa_id")

## Process dsec2 data to get labour costs associated with other diversification (ds_paidl)
dsec2_data_process <- dsec2_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(ds2acode==21300) %>%  
  filter(ds2plc!=0) %>% 
  group_by(fa_id) %>% 
  summarise(ds_paidl=sum(ds2plc))

## Process ant data to get partners & directors labour (pdlab) and underpayment of family labour (uflab)
ant_data_process <- ant_data %>% 
  filter(fa_id %% 10000==sampyear) %>% 
  filter(nt_code %in% c("XNIP", "XNIU")) %>% 
  group_by(fa_id,nt_code) %>% 
  summarise(cost=sum(antcost)) %>% 
  pivot_wider(id_cols=fa_id, names_from = nt_code, values_from=cost) %>% 
  rename(i_pdlab = XNIP, i_uflab = XNIU)
  
  
## Join all the processed datasets together; replace any missing data with zeroes; calculate average hourly wages.
merged_data <- list(fa_data_process, alb_data_process, dsec1_data_process, dsec2_data_process, ant_data_process, avf_data_process) %>%  
  reduce(full_join, by="fa_id") %>% 
  left_join(weights, by="fa_id")
merged_data[is.na(merged_data)]=0 
merged_data <- merged_data %>% 
  mutate(f_rlabour=i_lab-(i_pdlab+i_uflab+ ds1_conlab + ecclab + ds_paidl + i_clab)) %>% 
  select(fa_id, fbswt, type, reg_labour_number, reg_labour_hours, reg_labour_wages = f_rlabour, cas_labour_number, cas_labour_hours, cas_labour_wages = i_clab) %>% 
  mutate(reg_labour_hourly_wage = reg_labour_wages/reg_labour_hours,cas_labour_hourly_wage = cas_labour_wages/cas_labour_hours)

## Create a summary by farmtype
summary_by_farmtype <- merged_data %>% 
  select(type, fbswt, reg_labour_number, reg_labour_hours, reg_labour_wages, cas_labour_number, cas_labour_hours, cas_labour_wages) %>% 
  group_by(type) %>% 
  summarise_at(vars(reg_labour_number:cas_labour_wages), list(~weighted.mean(., w=fbswt))) %>% 
  #Add row for all farms
  bind_rows(mutate(summarise_at(merged_data, vars(reg_labour_number:cas_labour_wages), list(~weighted.mean(., w=fbswt))), type=9)) %>% 
  #Hourly calculations
  mutate(reg_labour_hourly_wage = reg_labour_wages/reg_labour_hours, cas_labour_hourly_wage = cas_labour_wages/cas_labour_hours)


## Process census data to get total number of farms meeting FBS thresholds, by farm type, in order to scale up to national figures

names(census_data) <- tolower(names(census_data))
for (x in colnames(census_data)){
  attr(census_data[[deparse(as.name(x))]],"format.sas")=NULL
}
census_data_process <- census_data
census_data_process <- census_data_process %>%
  select(typhigh, typmed, typlow, robust_new, sumso, slr) %>% 
  mutate(census_type=case_when(
    typhigh %in% c(6:8)~8,
    robust_new==1~1,
    robust_new==2~2,
    robust_new==6~3,
    robust_new==7 & typlow %in% c(481,483,484)~4,
    typmed==9946|typmed==9947~5,
    robust_new==8~7,
    typlow==482~6,
    robust_new==9&typlow==484~8,
    robust_new==3~9,
    robust_new==4~10,
    robust_new==5~11,
    robust_new==10~12,
    robust_new==11~13
  )) %>%
  filter(is.na(census_type)==FALSE)
census_data_process[is.na(census_data_process)]=0
census_fbs_threshold <- census_data_process %>% 
  filter(sumso > FBS_euro_threshold*exchange_rate & slr > FBS_slr_threshold & census_type %in% 1:8)

census_farmtype_totals <- census_fbs_threshold %>%
  group_by(census_type) %>% 
  summarise(census_total = n())
census_farmtype_totals <- census_farmtype_totals %>% 
  bind_rows(c(census_type=9,colSums(census_farmtype_totals[,2]))) %>% 
  rename(type=census_type)

## Scale up to national figures the employee numbers, hours and wages by multiplying by the number of farms of each type
scaled_up_table <- summary_by_farmtype %>% 
  left_join(census_farmtype_totals, by="type")
for (i in 2:7){
scaled_up_table[i] <- scaled_up_table[i]*scaled_up_table$census_total
}
scaled_up_table <- scaled_up_table %>% 
  select(-census_total)




## Add "wordy" farmtypes; rearrange columns
setkey(setDT(merged_data),type)
merged_data[setDT(type_tab),farmtype:=i.type_words]
merged_data <- merged_data %>% 
  select(fa_id, farmtype, everything(), -type)
setkey(setDT(summary_by_farmtype),type)
summary_by_farmtype[setDT(type_tab), farmtype := i.type_words]
summary_by_farmtype <- summary_by_farmtype %>% 
  select(farmtype, everything(), -type)
setkey(setDT(scaled_up_table),type)
scaled_up_table[setDT(type_tab),farmtype:=i.type_words]
scaled_up_table <- scaled_up_table %>% 
  select(farmtype, everything(), -type)

## Output a csv file with the data.
output_tables <- list("Farm list" = merged_data, "Summary by farmtype" = summary_by_farmtype, "National figures" = scaled_up_table)
write_xlsx(output_tables,paste0("FBS_labour_data_", datayear, "_", sampyear-2000, ".xlsx"))
