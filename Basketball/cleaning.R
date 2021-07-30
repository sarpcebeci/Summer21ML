library(tidyverse) 
library(tidymodels)
library(RSQLite)
library(DBI)


# The Database is from: https://www.kaggle.com/wyattowalsh/basketball 
# Taken on 07-30-2021
setwd("~/Downloads/basketball")
con <- dbConnect(RSQLite::SQLite(), "basketball.sqlite")

# Queries 
 
player_bios_trans <- 
  dbGetQuery(
    con,
    "SELECT namePlayerBREF, slugPlayerBREF, nameTable, numberTransactionPlayer, 
    dateTransaction, isGLeagueMovement, isDraft, isSigned, isWaived, 
    isTraded FROM Player_Bios WHERE nameTable == 'Transactions'"
    )

player_bios_sal <- 
  dbGetQuery(
    con, 
    "SELECT namePlayerBREF, slugPlayerBREF, nameTable, slugSeason, nameTeam, 
    slugLeague, amountSalary FROM Player_Bios WHERE nameTable == 'Salaries'"
    )

game_home <- 
  dbGetQuery(
    con,
    "SELECT GAME_ID, SEASON_ID, TEAM_ID_HOME, WL_HOME, PTS_HOME, FGM_HOME, 
    FTM_HOME, FGA_HOME, FG_PCT_HOME, FG3M_HOME, FG3A_HOME, FG3_PCT_HOME, 
    OREB_HOME, DREB_HOME, AST_HOME, STL_HOME, BLK_HOME, TOV_HOME FROM Game"
    )

game_away <- 
  dbGetQuery(
    con, 
    "SELECT GAME_ID, SEASON_ID, TEAM_ID_AWAY, WL_AWAY, PTS_AWAY, FGM_AWAY , 
    FTM_AWAY , FGA_AWAY, FG_PCT_AWAY, FG3M_AWAY, FG3A_AWAY, FG3_PCT_AWAY, 
    OREB_AWAY, DREB_AWAY, AST_AWAY, STL_AWAY, BLK_AWAY, TOV_AWAY FROM Game"
    )

Player_Attr <- dbGetQuery(con, "SELECT * FROM Player_Attributes")


# Cleaning And Combining

player_bios_trans1 <- 
  player_bios_trans %>% 
  mutate(
    dateTransaction = as.Date(dateTransaction, origin = "1970-01-01 UTC"),
    Year = as.character(dateTransaction) %>% substr(1, 4) %>% as.numeric()
    )

player_bios_combined <- 
  player_bios_sal %>% 
  select(-nameTable) %>% 
  mutate(
    Year = substr(slugSeason, 1, 4) %>% as.numeric()
    ) %>% 
  left_join(., 
            player_bios_trans1 %>% 
              select(-nameTable, -namePlayerBREF, -dateTransaction), 
            by = c("slugPlayerBREF" = "slugPlayerBREF","Year" = "Year")
            ) %>% 
  mutate(
    numberTransactionPlayer = zoo::na.locf(numberTransactionPlayer)
    ) %>% 
  mutate_all(~replace(., is.na(.), 0)) 

bios_attr <- 
  player_bios_combined %>% 
  left_join(., 
            Player_Attr, 
            by = c("namePlayerBREF" = "DISPLAY_FIRST_LAST")
            ) %>% 
  select(-PIE) %>% 
  mutate(
    SCHOOL = if_else(is.na(SCHOOL), "Unknown", SCHOOL),
    ALL_STAR_SIDE_SHOWS = if_else(is.na(ALL_STAR_APPEARANCES), 1, 0),
    ALL_STAR_APPEARANCES = if_else(is.na(ALL_STAR_APPEARANCES), 0, ALL_STAR_APPEARANCES)
    ) %>% 
  filter(!is.na(NBA_FLAG)) %>% 
  mutate(
    PTS = if_else(is.na(PTS), 0, PTS),
    AST = if_else(is.na(AST), 0, AST),
    REB = if_else(is.na(REB), 0, REB)
    )

game_home[,c(8,10,11,13:18)] <- 
  game_home[,c(8,10,11,13:18)] %>% 
  mutate_if(is.character, as.numeric)

## Handling Missing W 

game_home[which(is.na(game_home$WL_HOME)),] <- 
  game_home[which(is.na(game_home$WL_HOME)),] %>% 
  mutate(WL_HOME = if_else(PTS_HOME < 100, "L", "W"))

## Handling Missing Team Stats

game_home_imputed <- 
  game_home %>% 
  recipe(WL_HOME ~ ., data = .) %>% 
  update_role(GAME_ID, new_role = "id variable") %>% 
  step_mutate(
    SEASON_ID = as.factor(SEASON_ID),
    TEAM_ID_HOME = as.factor(TEAM_ID_HOME)
    ) %>%
  step_other(TEAM_ID_HOME, threshold = .08) %>%
  step_other(SEASON_ID, threshold = .016) %>% 
  step_impute_linear(
    FGA_HOME:TOV_HOME,
    impute_with = imp_vars(starts_with("SEASON_ID"),
                           starts_with("TEAM_ID_HOME"),
                           PTS_HOME)
    ) %>% prep() %>% juice()

game_home[,c(8:18)] <- game_home_imputed[,c(7:17)]

game_home_agg <- game_home %>% 
  mutate(WL_HOME = if_else(WL_HOME == "L", 0, 1)) %>% 
  group_by(TEAM_ID_HOME, SEASON_ID) %>% 
  summarise_if(is.numeric, mean, na.rm = TRUE)

game_away[,c(8,10,11,13:18)] <- 
  game_away[,c(8,10,11,13:18)] %>% 
  mutate_if(is.character, as.numeric)

game_away[which(is.na(game_away$WL_AWAY)),] <- 
  game_away[which(is.na(game_away$WL_AWAY)),] %>% 
  mutate(WL_AWAY = if_else(PTS_AWAY < 100, "L", "W"))

game_away_imputed <- 
  game_away %>% 
  recipe(WL_AWAY ~ ., data = .) %>% 
  update_role(GAME_ID, new_role = "id variable") %>% 
  step_mutate(
    SEASON_ID = as.factor(SEASON_ID),
    TEAM_ID_AWAY = as.factor(TEAM_ID_AWAY)
    ) %>%
  step_other(TEAM_ID_AWAY, threshold = .08) %>%
  step_other(SEASON_ID, threshold = .016) %>% 
  step_impute_linear(
    FGA_AWAY:TOV_AWAY, 
    impute_with = imp_vars(starts_with("SEASON_ID"),
                           starts_with("TEAM_ID_AWAY"),
                           PTS_AWAY)
    ) %>% prep() %>% juice()

game_away[,c(8:18)] <- game_away_imputed[,c(7:17)]

game_away_agg <- game_away %>% 
  mutate(WL_AWAY = if_else(WL_AWAY == "L", 0, 1)) %>% 
  group_by(TEAM_ID_AWAY, SEASON_ID) %>% 
  summarise_if(is.numeric, mean, na.rm = TRUE)

game <- 
  game_home_agg %>% 
  left_join(., game_away_agg, 
            by = c("TEAM_ID_HOME" = "TEAM_ID_AWAY", 
                   "SEASON_ID" = "SEASON_ID")
            ) %>% 
  mutate(SEASON_ID = substr(SEASON_ID, 2, 5) %>% as.numeric())

data <- 
  bios_attr %>% 
  left_join(., ungroup(game), 
            by = c("TEAM_ID" = "TEAM_ID_HOME", 
                   "Year" = "SEASON_ID")
            ) %>% 
  mutate(BIRTHYEAR = substr(BIRTHDATE, 1, 4) %>% as.numeric)

## Handling Missing Height and Weight

data_raw <- 
  data %>% 
  recipe(PTS ~ . , data = .) %>% 
  step_mutate(POSITION = as.factor(POSITION)) %>% 
  step_dummy(POSITION, keep_original_cols = TRUE) %>% 
  step_impute_linear(
    HEIGHT, WEIGHT,
    impute_with = imp_vars(Year, BIRTHYEAR, starts_with("POSITION"))
    ) %>% 
  step_impute_mean(WL_HOME:TOV_AWAY) %>% 
  prep() %>% juice() 

## Selecting Columns From Raw

data <- data_raw %>% 
  select(
    ID, DISPLAY_FI_LAST, nameTeam, amountSalary, PTS, BIRTHYEAR, Year:isTraded, 
    SCHOOL:GAMES_PLAYED_CURRENT_SEASON_FLAG, FROM_YEAR:TOV_AWAY
    ) %>% 
  mutate(id_row = row_number())


## REducing Dimensionality

pca_id <- 
  data %>% 
  select(id_row, WL_HOME:TOV_AWAY) %>% 
  recipe(~., data = .) %>% 
  update_role(id_row, new_role = "id variable") %>% 
  step_normalize(all_predictors()) %>% 
  step_pca(all_predictors(), 
           threshold = .90) %>% 
  prep() %>% juice()

data <- left_join(data, pca_id, by = "id_row")

## Import the Data

data %>% write_csv("data.csv")