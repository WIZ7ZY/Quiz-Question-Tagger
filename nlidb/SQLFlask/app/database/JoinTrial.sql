--
-- Database: `samplevideo_db`
--
-- --------------------------------------------------------
--
-- Table structure for table `user_details`
--
CREATE TABLE IF NOT EXISTS `registered` (
  `students` varchar(11) DEFAULT,
  `subject` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`students`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=10001 ;
--
-- Dumping data for table `user_details`
--
INSERT INTO `registered` (`students`, `subject`) VALUES
('rogers63', 'science'),
('mike28', 'science');

CREATE TABLE IF NOT EXISTS `dept` (
  `students` varchar(11) DEFAULT,
  `deptid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`students`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1 AUTO_INCREMENT=10001 ;
--
-- Dumping data for table `user_details`
--
INSERT INTO `dept` (`students`, `deptid`) VALUES
('rogers63', 'comp'),
('mike28', 'comp');