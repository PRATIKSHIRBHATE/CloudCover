##**********************************************************************;
# Project           : Analyse the quality of production units

# Program name      : CloudCover_SolutionFIle.R

#Author            : Pratik Shirbhate

#Date created      : 2018-07-15

#Purpose           : Summarize production_log,Sales and Complaints data for the study.

#Revision History  :
#Date        Author      Ref    Revision (Date in YYYY-MM-DD format) 
#2018-07-16 Pratik S      1      Added comments to improve readability 

#**********************************************************************

# Below are the input data files and reports names used in this project
#Sales,complaints,Production_logs
#report1,report2,report3

#Import the Required Pacakages in R, make sure to download and install the packages with install.packages(package_name) before importing
#tydr package to clean up the data
#dplyr to manipulate the data
#sqldf to fethch the required data from the the output datasets

library("tidyr", lib.loc="/Library/Frameworks/R.framework/Versions/3.4/Resources/library")
library("dplyr", lib.loc="/Library/Frameworks/R.framework/Versions/3.4/Resources/library")
library("sqldf", lib.loc="/Library/Frameworks/R.framework/Versions/3.4/Resources/library")

# Sales and Production_logs files has some columns containing the data in partial jason format
# To parse the jason data #jasonlite package is used, the code for cleaning data is 
# availbale as a seperate script file, the ouput of the cleaned data is exported as csv files to 
# local system and used in the below script

# Import the required input files and give the proper column names
Sales <- read.csv("~/Documents/CloudCover/Sales_final.csv", header=FALSE)
colnames(Sales) <- c("Invoice_Id","Customer_Id","Batch_Id","Items1","Item1_count","Items2","Item2_count","Items3","Item3_count")

complaints <- read.delim("~/Documents/CloudCover/Complaints.tsv", header=FALSE)
colnames(complaints) <- c("Invoice_Id","Defective_Item")

Production_logs <- read.csv("~/Documents/CloudCover/Production_logs1.csv", header=FALSE)
colnames(Production_logs) <-c ("Production_Unit_Id","Batch_Id","scissor_produced","No_sissor_produced","paper_produced","No_paper_produced","rock_produced","No_rock_produced","scissor_discarded","No_scissor_discarded","paper_discarded","No_paper_discarded","rock_discarded","No_rock_discarded")

# Data for scissor,paper,rock are analysed seperately and stored as intermediate outputs
# These intermediate outputs are used for generating the final outputs/reports

#runing an sql queries to get the Defect_percent for scissors
# get the number of defected iterms detected and removed from production_logs file

table1 <- sqldf("select Production_Unit_Id,scissor_discarded Item,No_scissor_discarded Defect_count,No_sissor_produced production from Production_logs;")

# get the number of scissors found defective from complaints
# To get the exact count, complaints dataframe is joined with sales on invoice_Id and then result
# Result is joined with production_logs on Batch_Id

table2 <- sqldf("select Production_Unit_Id,  Defective_Item Item,
                case when Items1 = 'scissor' then Item1_count  
               when Items2 = 'scissor' then Item2_count 
               when Items3 = 'scissor' then Item3_count else 0 end as Defect_count, No_sissor_produced
                from complaints c join Sales s on c.Invoice_Id = s.Invoice_Id
                join Production_logs p on p.Batch_Id = s.Batch_Id where Defective_Item = 'scissor'")

# get the total number of defective scissors

table3 <- sqldf("select * from table1 union all select * from table2")
table4 <- sqldf("select Production_Unit_Id,Item,sum(Defect_count) as Defect_count,sum(production) as production from table3 group by Production_Unit_Id,Item")

#get the % of defective scissors

table5 <- sqldf("select Production_Unit_Id,Item,round(((Defect_count)*100/(production)),2) as Defect_percent from table4")


#runing an sql queries to get the Defect_percent for paper
# get the number of defected iterms detected and removed

table6 <- sqldf("select Production_Unit_Id,paper_discarded Item,No_paper_discarded Defect_count,No_paper_produced production from Production_logs;")

# get the number of papers found defective from complaints

table7 <- sqldf("select Production_Unit_Id,  Defective_Item Item,
                case when Items1 = 'paper' then Item1_count  
                when Items2 = 'paper' then Item2_count 
                when Items3 = 'paper' then Item3_count else 0 end as Defect_count, No_sissor_produced
                from complaints c join Sales s on c.Invoice_Id = s.Invoice_Id
                join Production_logs p on p.Batch_Id = s.Batch_Id where Defective_Item = 'paper'")

# get the total number of defective papers

table8 <- sqldf("select * from table6 union all select * from table7")
table9 <- sqldf("select Production_Unit_Id,Item,sum(Defect_count) as Defect_count,sum(production) as production from table8 group by Production_Unit_Id,Item")

#get the % of defective papers
table10 <- sqldf("select Production_Unit_Id,Item,round(((Defect_count)*100/(production)),2) as Defect_percent from table9")


#runing an sql queries to get the Defect_percent for rock
# get the number of defected rocks detected and removed

table11 <- sqldf("select Production_Unit_Id,rock_discarded Item,No_rock_discarded Defect_count,No_rock_produced production from Production_logs;")

# get the number of rock found defective from complaints

table12 <- sqldf("select Production_Unit_Id,  Defective_Item Item,
                case when Items1 = 'rock' then Item1_count  
                when Items2 = 'rock' then Item2_count 
                when Items3 = 'rock' then Item3_count else 0 end as Defect_count, No_rock_produced
                from complaints c join Sales s on c.Invoice_Id = s.Invoice_Id
                join Production_logs p on p.Batch_Id = s.Batch_Id where Defective_Item = 'rock'")

# get the total number of defective rock

table13 <- sqldf("select * from table11 union all select * from table12")
table14 <- sqldf("select Production_Unit_Id,Item,sum(Defect_count) as Defect_count,sum(production) as production from table13 group by Production_Unit_Id,Item")

#get the % of defective rock
table15 <- sqldf("select Production_Unit_Id,Item,round(((Defect_count)*100/(production)),2) as Defect_percent from table14")

#combining the outputs for scissor,paper and rock
output <- union(table5,table10)
output1 <- union(output,table15)

#exporting the result as report1

write.csv(output1, file = "~/Documents/CloudCover/report1.csv")

#list of production units with overall defective production of more than 20% of all items produced
# combining the results of scissor,paper and rock

output3 <- union(table4,table9)
output4 <- union(output3,table14)
output5 <- sqldf("select Production_Unit_Id,sum(Defect_count) as Total_Defect_count,sum(production) as Total_production from output4 group by Production_Unit_Id")
output6 <- sqldf("select Production_Unit_Id, (Total_Defect_count*100/Total_production) as Defect_percent from output5 ")
output7 <- sqldf("select Production_Unit_Id,Defect_percent from output6 where Defect_percent > 20")

write.csv(output7, file = "~/Documents/CloudCover/report2.csv")

#percentage of total defective items that were detected by Quality Control on the factory floor

df1 <- sqldf("select Production_Unit_Id,(No_scissor_discarded+No_paper_discarded+No_rock_discarded) as QA_Defect_count from Production_logs;")
df2 <- sqldf("select df1.Production_Unit_Id,QA_Defect_count,Total_Defect_count from df1 join output5 on df1.Production_Unit_Id = output5.Production_Unit_Id")
df3 <- sqldf("select Production_Unit_Id,(QA_Defect_count*100/Total_Defect_count) as percent_detected_defects_by_QA from df2 order by percent_detected_defects_by_QA desc")

write.csv(df3, file = "~/Documents/CloudCover/report3.csv")

##### End of the project script
##### Modified input files and reports will be sent along with script file

