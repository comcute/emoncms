<?php

// This timeseries engine implements:
// Fixed Interval No Averaging
#define('AVG', 'True');
class EmonLogger {
public function warn($msg) {
echo $msg . "\n";
}
public function info($msg) {
$this->warn($msg);
}
}
class PHPCIRC {
	private $dir;
	private $log;

	/**
	 * Constructor.
	 *
	 * @api
	 */

	public function __construct($settings) {
		if (isset($settings['datadir'])) {
			$this->dir = $settings['datadir'];
		}

		else $this->dir = __DIR__."/";
		// else {
		// 	$this->dir = "/var/lib/PHPFic/";
		// }

		$this->log = new EmonLogger();
		#$this->log = new EmonLogger(__FILE__);
	}

	/**
	 * Create feed
	 *
	 * @param integer $id The id of the feed to be created
	 */
	public function create($id, $options) {
		$interval = (int) $options['interval'];
		if ($interval < 4) {
			$interval = 4;
		}

		// Check to ensure we dont overwrite an existing feed
		if (!$meta = $this->get_meta($id)) {
			// Set initial feed meta data
			$meta = new stdClass();
			$meta->max_npoints = 6307200; // 1 year if interval = 5
			$meta->end_pos = 0;
			$meta->interval = $interval;
			$meta->end_time = 0;

			// Save meta data
			$this->create_meta($id, $meta);

			$fh = @fopen($this->dir . $id . ".dat", 'c+');

			if (!$fh) {
				$this->log->warn("PHPFic:create could not create data file id=$id");
				return false;
			}

			// Fill padding buffer
			$pointsperblock = $meta->max_npoints / 4;
			$buf = '';
			for ($n = 0; $n < $pointsperblock; $n++) {
				$buf .= pack("f", NAN);
			}

			//write padding
			for ($i = 0; $i < ($meta->max_npoints / $pointsperblock); $i++) {
				fwrite($fh, $buf);
			}

			fclose($fh);
		}

		$feedname = "$id.meta";
		if (file_exists($this->dir . $feedname)) {
			return true;
		} else {
			$this->log->warn("PHPFic:create failed to create feed id=$id");
			return false;
		}
	}

	/**
	 * Adds a data point to the feed
	 *
	 * @param integer $id The id of the feed to add to
	 * @param integer $time The unix timestamp of the data point, in seconds
	 * @param float $value The value of the data point
	 */
	public function post($id, $timestamp, $value) {
		$this->log->info("PHPFic:post post id=$id timestamp=$timestamp value=$value");

		$id = (int) $id;
		$timestamp = (int) $timestamp;
		$value = (float) $value;

		$now = time();
		$start = $now - (3600 * 24 * 365 * 5); // 5 years in past
		$end = $now + (3600 * 48); // 48 hours in future

		if ($timestamp < $start || $timestamp > $end) {
			$this->log->warn("PHPFic:post timestamp out of range");
			#return false;
		}

		// If meta data file does not exist then exit
		if (!$meta = $this->get_meta($id)) {
			$this->log->warn("PHPFic:post failed to fetch meta id=$id");
			return false;
		}

		$need_padding = False;

		// Calculate interval that this datapoint belongs too
		$timestamp = floor($timestamp / $meta->interval) * $meta->interval;

		// If this is a new feed (npoints == 0) then set the start time to the current datapoint
		if ($meta->end_time == 0) {
			$meta->end_time = $timestamp;
			$this->create_meta($id, $meta);
		}

		// Calculate position in base data file of datapoint
		$pos = (floor(($timestamp - $meta->end_time) / $meta->interval) + $meta->end_pos);

		// Calculate circular position
		if ($pos < 0) {
			$pos = $pos + $meta->max_npoints;
		} elseif ($pos > $meta->max_npoints - 1) {
			// Check if padding is necessary
			if ($pos - $meta->end_pos > 1) {
				$need_padding = True;
			}

			$pos = $pos - $meta->max_npoints;
		} else {
			// Check if padding is necessary
			if ($pos - $meta->end_pos > 1) {
				$need_padding = True;
			}

		}

		$fh = fopen($this->dir . $id . ".dat", 'c+');
		if (!$fh) {
			$this->log->warn("PHPFic:post could not open data file id=$id");
			return false;
		}

		// write padding if necessary
		if ($need_padding) {
			$this->write_padding($fh, $pos, $meta);
		}

		// Write new datapoint
		fseek($fh, $pos * 4);
		if (!is_nan($value)) {
			fwrite($fh, pack("f", $value));
		} else {
			fwrite($fh, pack("f", NAN));
		}

		// Close file
		fclose($fh);

		// Update time & pos if timespamp in future
		if ($timestamp > $meta->end_time) {
			$meta->end_pos = $pos;
			$meta->end_time = $timestamp;
		}

		$this->update_meta($id, $meta);

		return $value;
	}

	/**
	 * Updates a data point in the feed
	 *
	 * @param integer $id The id of the feed to add to
	 * @param integer $time The unix timestamp of the data point, in seconds
	 * @param float $value The value of the data point
	 */
	public function update($id, $timestamp, $value) {
		return $this->post($id, $timestamp, $value);
	}

	/**
	 * Return the data for the given timerange
	 *
	 * @param integer $id The id of the feed to fetch from
	 * @param integer $start The unix timestamp in ms of the start of the data range
	 * @param integer $end The unix timestamp in ms of the end of the data range
	 * @param integer $dp The number of data points to return (used by some engines)
	 */
	public function get_data($id, $start, $end, $outinterval) {
		$id = intval($id);
		$start = intval($start / 1000);
		$end = intval($end / 1000);
		$outinterval = (int) $outinterval;

		// If meta data file does not exist then exit
		if (!$meta = $this->get_meta($id)) {
			return false;
		}

		if ($outinterval < $meta->interval) {
			$outinterval = $meta->interval;
		}
		if ($end > $meta->end_time) {
			$end = $meta->end_time;
		}

		$dp = ceil(($end - $start) / $outinterval);
		#$end = $start + ($dp * $outinterval);

		// $dpratio = $outinterval / $meta->interval;
		if ($dp < 1) {
			return false;
		}
		// The number of datapoints in the query range:
		$dp_in_range = ($end - $start) / $meta->interval;

		// Divided by the number we need gives the number of datapoints to skip
		// i.e if we want 1000 datapoints out of 100,000 then we need to get one
		// datapoints every 100 datapoints.
		$skipsize = floor($dp_in_range / $dp);
		if ($skipsize < 1) {
			$skipsize = 1;
		}

		// Calculate the starting datapoint position
		$start_time = $meta->end_time - $meta->interval * $meta->max_npoints;

		if ($start < $meta->end_time) {
			if ($start > $start_time) {
				$startpos = $meta->end_pos - ceil(($meta->end_time - $start) / $meta->interval);
			} else {
				$startpos = $meta->end_pos - $meta->max_npoints + 1;
			}

		} else {
			$startpos = $meta->end_pos;
		}

		if ($startpos < 0) {
			$startpos = $startpos + $meta->max_npoints;
		}

		$data = array();
		$time = 0;
		$i = 0;

		// The datapoints are selected within a loop that runs until we reach a
		// datapoint that is beyond the end of our query range
		$fh = fopen($this->dir . $id . ".dat", 'rb');
		//$this->log->warn("PHPCIRC:start=$start, end=$end, int=$outinterval, dps=$dp_in_range, start_time=$start_time, end_time=$meta->end_time");

		while (($dp_count = $i * $skipsize) < $dp_in_range) {
			// $position steps forward by skipsize every loop
			$pos = ($startpos + $dp_count);
			if ($pos > ($meta->max_npoints - 1)) {
				$pos = $pos - $meta->max_npoints;
			}

			// Exit the loop if the position is beyond the end of the file
			if ($pos > $meta->max_npoints - 1) {
				break;
			}
			//$this->log->warn("pos2=$pos, time=$time");
			// read from the file
			fseek($fh, $pos * 4);
			$val = unpack("f", fread($fh, 4));

			// calculate the datapoint time
			if ($pos <= $meta->end_pos) {
				$time = $meta->end_time - (($meta->end_pos - $pos) * $meta->interval);
			} else {
				$time = $start_time + (($pos - $meta->end_pos) * $meta->interval);
			}
			//fixme

			// add to the data array if its not a nan value
			if (!is_nan($val[1])) {
				$data[] = array($time * 1000, $val[1]);
			}

			$i++;
			#	$arr = get_defined_vars();
			#	print_r($arr);
			#	if($i>3)
			#	return;
		}
		return $data;
	}

	/**
	 * Get the last value from a feed
	 *
	 * @param integer $id The id of the feed
	 */
	public function lastvalue($id) {
		$id = (int) $id;

		// If meta data file does not exist then exit
		if (!$meta = $this->get_meta($id)) {
			return false;
		}

		$fh = fopen($this->dir . $id . ".dat", 'rb');
		$pos = $meta->end_pos;
		fseek($fh, $pos * 4);
		$d = fread($fh, 4);
		fclose($fh);

		$val = unpack("f", $d);
		$time = date("Y-n-j H:i:s", $meta->end_time);

		return array('time' => $time, 'value' => $val[1]);

	}

	public function export($id, $start) {
		$id = (int) $id;
		$start = (int) $start;

		$feedname = $id . ".dat";

		// If meta data file does not exist then exit
		if (!$meta = $this->get_meta($id)) {
			$this->log->warn("PHPFic:post failed to fetch meta id=$id");
			return false;
		}

		// There is no need for the browser to cache the output
		header("Cache-Control: no-cache, no-store, must-revalidate");

		// Tell the browser to handle output as a csv file to be downloaded
		header('Content-Description: File Transfer');
		header("Content-type: application/octet-stream");
		header("Content-Disposition: attachment; filename={$feedname}");

		header("Expires: 0");
		header("Pragma: no-cache");

		// Write to output stream
		$fh = @fopen('php://output', 'w');

		$primary = fopen($this->dir . $feedname, 'rb');
		$primarysize = filesize($this->dir . $feedname);

		$localsize = $start;
		$localsize = intval($localsize / 4) * 4;
		if ($localsize < 0) {
			$localsize = 0;
		}

		// Get the first point which will be updated rather than appended
		if ($localsize >= 4) {
			$localsize = $localsize - 4;
		}

		fseek($primary, $localsize);
		$left_to_read = $primarysize - $localsize;
		if ($left_to_read > 0) {
			do {
				if ($left_to_read > 8192) {
					$readsize = 8192;
				} else {
					$readsize = $left_to_read;
				}

				$left_to_read -= $readsize;

				$data = fread($primary, $readsize);
				fwrite($fh, $data);
			} while ($left_to_read > 0);
		}
		fclose($primary);
		fclose($fh);
		exit;

	}

	public function delete($id) {
		if (!$meta = $this->get_meta($id)) {
			return false;
		}

		unlink($this->dir . $id . ".meta");
		unlink($this->dir . $id . ".dat");
	}

	public function get_feed_size($id) {
		if (!$meta = $this->get_meta($id)) {
			return false;
		}

		return (filesize($this->dir . $id . ".meta") + filesize($this->dir . $id . ".dat"));
	}

	public function get_meta($id) {
		$id = (int) $id;
		$feedname = "$id.meta";

		if (!file_exists($this->dir . $feedname)) {
			$this->log->warn("PHPFic:get_meta meta file does not exist id=$id");
			return false;
		}

		$meta = new stdClass();
		$metafile = fopen($this->dir . $feedname, 'rb');

		$tmp = unpack("I", fread($metafile, 4));
		$meta->max_npoints = $tmp[1];

		$tmp = unpack("I", fread($metafile, 4));
		$meta->end_pos = $tmp[1];

		$tmp = unpack("I", fread($metafile, 4));
		$meta->interval = $tmp[1];

		$tmp = unpack("I", fread($metafile, 4));
		$meta->end_time = $tmp[1];

		fclose($metafile);

		clearstatcache($this->dir . $id . ".dat");
		$filesize = filesize($this->dir . $id . ".dat");

		return $meta;
	}

	private function create_meta($id, $meta) {
		$id = (int) $id;

		$feedname = "$id.meta";
		$metafile = fopen($this->dir . $feedname, 'wb');

		if (!$metafile) {
			$this->log->warn("PHPFic:create_meta could not open meta data file id=" . $id);
			return false;
		}

		if (!flock($metafile, LOCK_EX)) {
			$this->log->warn("PHPFic:create_meta meta file id=" . $id . " is locked by another process");
			fclose($metafile);
			return false;
		}

		fwrite($metafile, pack("I", $meta->max_npoints));
		fwrite($metafile, pack("I", $meta->end_pos));
		fwrite($metafile, pack("I", $meta->interval));
		fwrite($metafile, pack("I", $meta->end_time));
		fclose($metafile);
	}

	private function update_meta($id, $meta) {
		$id = (int) $id;

		$feedname = "$id.meta";
		$metafile = fopen($this->dir . $feedname, 'c+');

		if (!$metafile) {
			$this->log->warn("PHPFic:update_meta could not open meta data file id=" . $id);
			return false;
		}

		if (!flock($metafile, LOCK_EX)) {
			$this->log->warn("PHPFic:update_meta meta file id=" . $id . " is locked by another process");
			fclose($metafile);
			return false;
		}

		fseek($metafile, 4);
		fwrite($metafile, pack("I", $meta->end_pos));
		fseek($metafile, 4, SEEK_CUR);
		fwrite($metafile, pack("I", $meta->end_time));
		fclose($metafile);
	}

	private function write_padding($fh, $pos, $meta) {
		$buf = '';
		if ($pos > $meta->end_pos) {
			$len = $pos - $meta->end_pos + 1;
			// Fill padding buffer
			for ($n = 0; $n < $len; $n++) {
				$buf .= pack("f", NAN);
			}

			fseek($fh, 4 * ($meta->end_pos + 1));
			//write padding
			fwrite($fh, $buf);
		} else {
			$len = $meta->max_npoints - ($meta->end_pos + 1);
			// Fill padding buffer
			for ($n = 0; $n < $len; $n++) {
				$buf .= pack("f", NAN);
			}

			fseek($fh, 4 * ($meta->end_pos + 1));
			//write padding
			fwrite($fh, $buf);
			$buf = '';
			$len = $pos;
			// Fill padding buffer
			for ($n = 0; $n < $len; $n++) {
				$buf .= pack("f", NAN);
			}
			fseek($fh, 0);
			//write padding
			fwrite($fh, $buf);
		}

	}

	public function csv_export($id, $start, $end, $outinterval) {
		$id = intval($id);
		$start = intval($start);
		$end = intval($end);
		$outinterval = (int) $outinterval;

		// If meta data file does not exist then exit
		if (!$meta = $this->get_meta($id)) {
			return false;
		}

		if ($outinterval < $meta->interval) {
			$outinterval = $meta->interval;
		}

		$dp = ceil(($end - $start) / $outinterval);
		$end = $start + ($dp * $outinterval);

		// $dpratio = $outinterval / $meta->interval;
		if ($dp < 1) {
			return false;
		}

		// The number of datapoints in the query range:
		$dp_in_range = ($end - $start) / $meta->interval;

		// Divided by the number we need gives the number of datapoints to skip
		// i.e if we want 1000 datapoints out of 100,000 then we need to get one
		// datapoints every 100 datapoints.
		$skipsize = round($dp_in_range / $dp);
		if ($skipsize < 1) {
			$skipsize = 1;
		}

		// Calculate the starting datapoint position in the timestore file
		if ($start > $meta->end_time) {
			$startpos = ceil(($start - $meta->end_time) / $meta->interval);
		} else {
			$start = ceil($meta->end_time / $outinterval) * $outinterval;
			$startpos = ceil(($start - $meta->end_time) / $meta->interval);
		}

		$data = array();
		$time = 0;
		$i = 0;

		// There is no need for the browser to cache the output
		header("Cache-Control: no-cache, no-store, must-revalidate");

		// Tell the browser to handle output as a csv file to be downloaded
		header('Content-Description: File Transfer');
		header("Content-type: application/octet-stream");
		$filename = $id . ".csv";
		header("Content-Disposition: attachment; filename={$filename}");

		header("Expires: 0");
		header("Pragma: no-cache");

		// Write to output stream
		$exportfh = @fopen('php://output', 'w');

		// The datapoints are selected within a loop that runs until we reach a
		// datapoint that is beyond the end of our query range
		$fh = fopen($this->dir . $id . ".dat", 'rb');
		while ($time <= $end) {
			// $position steps forward by skipsize every loop
			$pos = ($startpos + ($i * $skipsize));

			// Exit the loop if the position is beyond the end of the file
			if ($pos > $meta->npoints - 1) {
				break;
			}

			// read from the file
			fseek($fh, $pos * 4);
			$val = unpack("f", fread($fh, 4));

			// calculate the datapoint time
			$time = $meta->end_time + $pos * $meta->interval;

			// add to the data array if its not a nan value
			if (!is_nan($val[1])) {
				fwrite($exportfh, $time . "," . number_format($val[1], 2) . "\n");
			}

			$i++;
		}
		fclose($exportfh);
		exit;
	}

}

$api = new PHPCIRC(array());
#$api->create(148, array('interval' => 5));
#$api->post(148, $argv[1], $argv[2]);
#print_r($api->lastvalue(148));
$data = $api->get_data(148, $argv[1], $argv[2], $argv[3]);
var_dump($data);