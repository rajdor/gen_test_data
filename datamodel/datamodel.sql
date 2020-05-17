-- MySQL Script generated by MySQL Workbench
-- Sat 09 May 2020 09:59:56 PM AEST
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema staging
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema staging
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `staging` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `staging` ;

-- -----------------------------------------------------
-- Table `staging`.`ref_address`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`ref_address` (
  `idaddress` BIGINT NOT NULL,
  `gnaf` VARCHAR(15) NULL DEFAULT NULL,
  `building_name` VARCHAR(256) NULL DEFAULT NULL,
  `lot` VARCHAR(64) NULL DEFAULT NULL,
  `flat` VARCHAR(128) NULL DEFAULT NULL,
  `level` VARCHAR(128) NULL DEFAULT NULL,
  `street` VARCHAR(256) NULL DEFAULT NULL,
  `suburb` VARCHAR(128) NULL DEFAULT NULL,
  `state` CHAR(3) NULL DEFAULT NULL,
  `postcode` CHAR(4) NULL DEFAULT NULL,
  PRIMARY KEY (`idaddress`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`claim_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`claim_status` (
  `idclaim_status` BIGINT NOT NULL,
  `code` CHAR(10) NOT NULL,
  `description` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`idclaim_status`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`customer`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`customer` (
  `idcustomer` BIGINT NOT NULL,
  `gender` CHAR(1) NULL DEFAULT NULL,
  `first_name` VARCHAR(64) NULL DEFAULT NULL,
  `last_name` VARCHAR(64) NULL DEFAULT NULL,
  `idaddress` BIGINT NULL DEFAULT NULL,
  `address_1` VARCHAR(128) NULL DEFAULT NULL,
  `address_2` VARCHAR(128) NULL DEFAULT NULL,
  `address_3` VARCHAR(128) NULL DEFAULT NULL,
  `address_state` VARCHAR(32) NULL DEFAULT NULL,
  `address_postcode` VARCHAR(32) NULL DEFAULT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NOT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NOT NULL,
  PRIMARY KEY (`idcustomer`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`employee`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`employee` (
  `idemployee` BIGINT NOT NULL,
  `gender` CHAR(1) NULL DEFAULT NULL,
  `first_name` VARCHAR(64) NULL DEFAULT NULL,
  `last_name` VARCHAR(64) NULL DEFAULT NULL,
  `employee_status` CHAR(1) NULL DEFAULT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NULL DEFAULT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NULL DEFAULT NULL,
  PRIMARY KEY (`idemployee`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`policy_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`policy_status` (
  `idpolicy_status` BIGINT NOT NULL,
  `code` CHAR(10) NOT NULL,
  `description` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`idpolicy_status`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`ref_vehicle`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`ref_vehicle` (
  `idvehicle` BIGINT NOT NULL,
  `drive` VARCHAR(64) NULL DEFAULT NULL,
  `engine_id` BIGINT NULL DEFAULT NULL,
  `engine_description` VARCHAR(64) NULL DEFAULT NULL,
  `fuel_type_1` VARCHAR(64) NULL DEFAULT NULL,
  `fuel_type_2` VARCHAR(64) NULL DEFAULT NULL,
  `make` VARCHAR(64) NULL DEFAULT NULL,
  `model` VARCHAR(64) NULL DEFAULT NULL,
  `transmission` VARCHAR(64) NULL DEFAULT NULL,
  `vehicle_class` VARCHAR(64) NULL DEFAULT NULL,
  `vehicle_year` BIGINT NULL DEFAULT NULL,
  PRIMARY KEY (`idvehicle`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`motor_policy`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`motor_policy` (
  `idpolicy` BIGINT NOT NULL,
  `idpolicy_status` BIGINT NULL DEFAULT NULL,
  `start_date` DATE NULL DEFAULT NULL,
  `end_date` DATE NULL DEFAULT NULL,
  `premium` DOUBLE NULL DEFAULT NULL,
  `idvehicle` BIGINT NULL DEFAULT NULL,
  `color` VARCHAR(64) NULL DEFAULT NULL,
  `registration` VARCHAR(20) NULL DEFAULT NULL,
  `risk_idaddress` BIGINT NULL DEFAULT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NOT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NOT NULL,
  PRIMARY KEY (`idpolicy`),
  INDEX `fk_motor_policy_3_idx` (`idpolicy_status` ASC) VISIBLE,
  INDEX `fk_motor_policy_4_idx` (`idvehicle` ASC) VISIBLE,
  CONSTRAINT `fk_motor_policy_3`
    FOREIGN KEY (`idpolicy_status`)
    REFERENCES `staging`.`policy_status` (`idpolicy_status`),
  CONSTRAINT `fk_motor_policy_4`
    FOREIGN KEY (`idvehicle`)
    REFERENCES `staging`.`ref_vehicle` (`idvehicle`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`motor_claim`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`motor_claim` (
  `idmotor_claim` BIGINT NOT NULL,
  `idclaim_status` BIGINT NULL DEFAULT NULL,
  `lodgement_date` DATE NULL DEFAULT NULL,
  `incident_timestamp` TIMESTAMP NULL DEFAULT NULL,
  `incident_description` VARCHAR(2048) NULL DEFAULT NULL,
  `incident_location` VARCHAR(255) NULL DEFAULT NULL,
  `incident_postcode` SMALLINT NULL DEFAULT NULL,
  `claim_estimate` DOUBLE NULL DEFAULT NULL,
  `weather_conditions` VARCHAR(128) NULL DEFAULT NULL,
  `injured_flag` CHAR(1) NULL DEFAULT NULL,
  `police_services_attend` CHAR(1) NULL DEFAULT NULL,
  `fire_services_attend` CHAR(1) NULL DEFAULT NULL,
  `ambulance_services_attend` CHAR(1) NULL DEFAULT NULL,
  `idpolicy` BIGINT NULL DEFAULT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NOT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NOT NULL,
  PRIMARY KEY (`idmotor_claim`),
  INDEX `fk_motor_claim_1_idx` (`idpolicy` ASC) VISIBLE,
  INDEX `fk_motor_claim_2_idx` (`idclaim_status` ASC) VISIBLE,
  CONSTRAINT `fk_motor_claim_1`
    FOREIGN KEY (`idpolicy`)
    REFERENCES `staging`.`motor_policy` (`idpolicy`),
  CONSTRAINT `fk_motor_claim_2`
    FOREIGN KEY (`idclaim_status`)
    REFERENCES `staging`.`claim_status` (`idclaim_status`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`party_role`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`party_role` (
  `idparty_role` BIGINT NOT NULL,
  `code` CHAR(3) NOT NULL,
  `description` VARCHAR(32) NOT NULL,
  PRIMARY KEY (`idparty_role`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`party`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`party` (
  `idparty` BIGINT NOT NULL,
  `idcustomer` BIGINT NOT NULL,
  `idparty_role` BIGINT NOT NULL,
  INDEX `fk_party_1_idx` (`idcustomer` ASC) VISIBLE,
  INDEX `fk_party_2_idx` (`idparty_role` ASC) VISIBLE,
  CONSTRAINT `fk_party_1`
    FOREIGN KEY (`idcustomer`)
    REFERENCES `staging`.`customer` (`idcustomer`),
  CONSTRAINT `fk_party_2`
    FOREIGN KEY (`idparty_role`)
    REFERENCES `staging`.`party_role` (`idparty_role`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`portfolio`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`portfolio` (
  `idportfolio` BIGINT NOT NULL,
  `idcustomer` BIGINT NOT NULL,
  `idpolicy` BIGINT NOT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NOT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NOT NULL,
  INDEX `fk_portfolio_1_idx` (`idpolicy` ASC) VISIBLE,
  INDEX `fk_portfolio_2_idx` (`idcustomer` ASC) VISIBLE,
  CONSTRAINT `fk_portfolio_1`
    FOREIGN KEY (`idpolicy`)
    REFERENCES `staging`.`motor_policy` (`idpolicy`),
  CONSTRAINT `fk_portfolio_2`
    FOREIGN KEY (`idcustomer`)
    REFERENCES `staging`.`customer` (`idcustomer`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`recovery_type`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`recovery_type` (
  `idrecovery_type` BIGINT NOT NULL,
  `code` CHAR(3) NOT NULL,
  `description` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`idrecovery_type`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`recovery`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`recovery` (
  `idrecovery` BIGINT NOT NULL,
  `idclaim` BIGINT NULL DEFAULT NULL,
  `idrecovery_type` BIGINT NULL DEFAULT NULL,
  `amount` DOUBLE NULL DEFAULT NULL,
  `transaction_timestamp` TIMESTAMP NULL DEFAULT NULL,
  `description` VARCHAR(128) NULL DEFAULT NULL,
  `create_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_idemployee` BIGINT NOT NULL,
  `update_timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_idemployee` BIGINT NOT NULL,
  PRIMARY KEY (`idrecovery`),
  INDEX `fk_recovery_1_idx` (`idclaim` ASC) VISIBLE,
  INDEX `fk_recovery_2_idx` (`idrecovery_type` ASC) VISIBLE,
  CONSTRAINT `fk_recovery_1`
    FOREIGN KEY (`idclaim`)
    REFERENCES `staging`.`motor_claim` (`idmotor_claim`),
  CONSTRAINT `fk_recovery_2`
    FOREIGN KEY (`idrecovery_type`)
    REFERENCES `staging`.`recovery_type` (`idrecovery_type`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`ref_colors`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`ref_colors` (
  `idcolor` BIGINT NOT NULL,
  `colorcode` CHAR(8) NOT NULL,
  `colorname` VARCHAR(128) NOT NULL,
  PRIMARY KEY (`idcolor`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`ref_first_names`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`ref_first_names` (
  `idfirst_name` BIGINT NOT NULL,
  `gender` CHAR(1) NOT NULL,
  `first_name` VARCHAR(128) NOT NULL,
  PRIMARY KEY (`idfirst_name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `staging`.`ref_last_names`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`ref_last_names` (
  `idlast_name` BIGINT NOT NULL,
  `last_name` VARCHAR(128) NOT NULL,
  PRIMARY KEY (`idlast_name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

USE `staging` ;

-- -----------------------------------------------------
-- Placeholder table for view `staging`.`claim_summary`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`claim_summary` (`Claim_date` INT, `claim_hour` INT, `as_at` INT, `claim_count` INT, `claim_estimate` INT);

-- -----------------------------------------------------
-- Placeholder table for view `staging`.`outstanding_recoveries`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`outstanding_recoveries` (`first_name` INT, `last_name` INT, `address_1` INT, `address_2` INT, `address_3` INT, `address_state` INT, `address_postcode` INT, `idpolicy` INT, `policy_status` INT, `idmotor_claim` INT, `claim_create_timestamp` INT, `claim_status` INT, `claim_estimate` INT, `recovered_amount` INT, `outstanding_recoveries` INT);

-- -----------------------------------------------------
-- Placeholder table for view `staging`.`renewing_policies`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `staging`.`renewing_policies` (`idpolicy` INT, `end_date` INT, `customer_state` INT, `as_at` INT, `sum(policy.premium)` INT, `policys` INT);

-- -----------------------------------------------------
-- View `staging`.`claim_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `staging`.`claim_summary`;
USE `staging`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`jarrod`@`%` SQL SECURITY DEFINER VIEW `staging`.`claim_summary` AS select cast(`claim`.`create_timestamp` as date) AS `Claim_date`,substr(cast(`claim`.`create_timestamp` as char charset utf8mb4),12,2) AS `claim_hour`,now() AS `as_at`,count(0) AS `claim_count`,sum(`claim`.`claim_estimate`) AS `claim_estimate` from `staging`.`motor_claim` `claim` where (`claim`.`create_timestamp` > (now() - interval 60 day)) group by cast(`claim`.`create_timestamp` as date),substr(cast(`claim`.`create_timestamp` as char charset utf8mb4),12,2),now();

-- -----------------------------------------------------
-- View `staging`.`outstanding_recoveries`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `staging`.`outstanding_recoveries`;
USE `staging`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`jarrod`@`%` SQL SECURITY DEFINER VIEW `staging`.`outstanding_recoveries` AS select `staging`.`customer`.`first_name` AS `first_name`,`staging`.`customer`.`last_name` AS `last_name`,`staging`.`customer`.`address_1` AS `address_1`,`staging`.`customer`.`address_2` AS `address_2`,`staging`.`customer`.`address_3` AS `address_3`,`staging`.`customer`.`address_state` AS `address_state`,`staging`.`customer`.`address_postcode` AS `address_postcode`,`policy`.`idpolicy` AS `idpolicy`,`staging`.`policy_status`.`description` AS `policy_status`,`claim`.`idmotor_claim` AS `idmotor_claim`,`claim`.`create_timestamp` AS `claim_create_timestamp`,`staging`.`claim_status`.`description` AS `claim_status`,`claim`.`claim_estimate` AS `claim_estimate`,sum(coalesce(`staging`.`recovery`.`amount`,0)) AS `recovered_amount`,(`claim`.`claim_estimate` - coalesce(sum(`staging`.`recovery`.`amount`),0)) AS `outstanding_recoveries` from ((((((`staging`.`customer` join `staging`.`portfolio` on((`staging`.`portfolio`.`idcustomer` = `staging`.`customer`.`idcustomer`))) join `staging`.`motor_policy` `policy` on((`staging`.`portfolio`.`idpolicy` = `policy`.`idpolicy`))) join `staging`.`policy_status` on((`staging`.`policy_status`.`idpolicy_status` = `policy`.`idpolicy_status`))) join `staging`.`motor_claim` `claim` on((`claim`.`idpolicy` = `policy`.`idpolicy`))) join `staging`.`claim_status` on(((`claim`.`idclaim_status` = `staging`.`claim_status`.`idclaim_status`) and (`staging`.`claim_status`.`code` in ('OPN','FIN','CLS'))))) left join `staging`.`recovery` on((`staging`.`recovery`.`idclaim` = `claim`.`idmotor_claim`))) group by `staging`.`customer`.`first_name`,`staging`.`customer`.`last_name`,`staging`.`customer`.`address_1`,`staging`.`customer`.`address_2`,`staging`.`customer`.`address_3`,`staging`.`customer`.`address_state`,`staging`.`customer`.`address_postcode`,`policy`.`idpolicy`,`staging`.`policy_status`.`description`,`claim`.`idmotor_claim`,`claim`.`create_timestamp`,`staging`.`claim_status`.`description`,`claim`.`claim_estimate`;

-- -----------------------------------------------------
-- View `staging`.`renewing_policies`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `staging`.`renewing_policies`;
USE `staging`;
CREATE  OR REPLACE ALGORITHM=UNDEFINED DEFINER=`jarrod`@`%` SQL SECURITY DEFINER VIEW `staging`.`renewing_policies` AS select `policy`.`idpolicy` AS `idpolicy`,`policy`.`end_date` AS `end_date`,`staging`.`customer`.`address_state` AS `customer_state`,now() AS `as_at`,sum(`policy`.`premium`) AS `sum(policy.premium)`,count(0) AS `policys` from ((`staging`.`motor_policy` `policy` join `staging`.`portfolio` on((`policy`.`idpolicy` = `staging`.`portfolio`.`idpolicy`))) join `staging`.`customer` on((`staging`.`customer`.`idcustomer` = `staging`.`portfolio`.`idcustomer`))) where ((`policy`.`end_date` > now()) and (`policy`.`end_date` < (now() + interval 60 day))) group by `policy`.`idpolicy`,`policy`.`end_date`,`staging`.`customer`.`address_state`,now();

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
