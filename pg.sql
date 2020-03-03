DROP TABLE IF EXISTS powerdata;
DROP SEQUENCE IF EXISTS powerdata_seq;
CREATE SEQUENCE  powerdata_seq;

CREATE TABLE powerdata (
  ID int NOT NULL DEFAULT NEXTVAL ('powerdata_seq'),                                                                                                                                                                                                        nowdatetime timestamp DEFAULT NULL,                                                                                                                                                                                                         GridCurrent double precision NOT NULL,                                                                                                                                                                                                                GridVoltage double precision NOT NULL,
  GridPower double precision NOT NULL,
  GridEnergy double precision NOT NULL,
  GridFrequency double precision NOT NULL,
  GridPowerFactor double precision NOT NULL,
  GridAlarm double precision NOT NULL,
  SolarCurrent double precision DEFAULT NULL,
  SolarVoltage double precision DEFAULT NULL,
  SolarPower double precision DEFAULT NULL,
  SolarEnergy double precision DEFAULT NULL,
  SolarFrequency double precision DEFAULT NULL,
  SolarPowerFactor double precision DEFAULT NULL,
  SolarAlarm double precision DEFAULT NULL,
  PRIMARY KEY (ID)
); 

