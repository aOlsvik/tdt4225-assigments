def getTableQueries():
  TABLES = {}
  TABLES['user'] = (
      "CREATE TABLE IF NOT EXISTS `user` ("
      "  `id` varchar(3) NOT NULL,"
      "  `has_labels` BOOL NOT NULL,"
      "  PRIMARY KEY (`id`)"
      ") ENGINE=InnoDB")

  TABLES['activity'] = (
      "CREATE TABLE IF NOT EXISTS `activity` ("
      "  `id` varchar(10) NOT NULL,"
      "  `user_id` varchar(3) NOT NULL,"
      "  `transportation_mode` varchar(20) DEFAULT NULL,"
      "  `start_date_time` datetime NOT NULL,"
      "  `end_date_time` datetime NOT NULL,"
      "  PRIMARY KEY (`id`), "
      "  CONSTRAINT `activity_ibfk_1` FOREIGN KEY (`user_id`) "
      "     REFERENCES `user` (`id`) ON DELETE CASCADE"
      ") ENGINE=InnoDB")

  TABLES['trackpoint'] = (
      "CREATE TABLE IF NOT EXISTS `trackpoint` ("
      "  `id` INT NOT NULL AUTO_INCREMENT,"
      "  `activity_id` varchar(10) NOT NULL,"
      "  `lat` double NOT NULL,"
      "  `lon` double NOT NULL,"
      "  `altitude` int NOT NULL,"
      "  `date_days` double NOT NULL,"
      "  `date_time` datetime NOT NULL,"
      "  PRIMARY KEY (`id`),"
      "  CONSTRAINT `trackpoint_ibfk_1` FOREIGN KEY (`activity_id`) "
      "     REFERENCES `activity` (`id`) ON DELETE CASCADE"
      ") ENGINE=InnoDB")
  return TABLES