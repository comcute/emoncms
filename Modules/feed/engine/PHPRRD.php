<?php

// This timeseries engine implements:
// Fixed Interval with Averaging

class PHPRRD {
	private $dir = "/var/lib/phprrd/";
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
		$this->log = new EmonLogger(__FILE__);
	}

	/**
	 * Create feed
	 *
	 * @param integer $id The id of the feed to be created
	 */
	public function create($id, $options) {

		if (file_exists($this->dir . $id . ".rrd")) {
			return true;
		}

		$interval = (int) $options['interval'];
		if ($interval < 5) {
			$interval = 5;
		}

 
        $rows = 6307200; // one year
        

		$now = time();
        $start= floor($now / $interval) * $interval;
		$rrd_options = array(
			"--start", $start,
			"--step", $interval,
			"DS:feed:GAUGE:" . $interval * 2 . ":U:U",
			"RRA:AVERAGE:0.5:1:".$rows,
			"RRA:AVERAGE:0.5:18:".$rows/18,
			"RRA:AVERAGE:0.5:126:".$rows/126,
			"RRA:AVERAGE:0.5:540:".$rows/540);

		if (!rrd_create($this->dir . $id . ".rrd", $rrd_options)) {
			$this->log->warn("PHPRRD:create could not create data file id=$id");
			return false;
		}
		return true;
	}

	/**
	 * Adds a data point to the feed
	 *
	 * @param integer $id The id of the feed to add to
	 * @param integer $time The unix timestamp of the data point, in seconds
	 * @param float $value The value of the data point
	 */
	public function post($id, $timestamp, $value) {
		$this->log->info("PHPRRD:post post id=$id timestamp=$timestamp value=$value");

		$id = (int) $id;
		$timestamp = (int) $timestamp;
		$value = (float) $value;

		$now = time();
		$start = $now - (3600 * 24 * 365 * 5); // 5 years in past
		$end = $now + (3600 * 48); // 48 hours in future

		if ($timestamp < $start || $timestamp > $end) {
			$this->log->warn("PHPRRD:post timestamp out of range");
			return false;
		}

		$rrd_options = array(
			$timestamp . ":" . $value);

		if (!rrd_update($this->dir . $id . ".rrd", $rrd_options)) {
			$this->log->warn("PHPRRD:could not update data file id=$id");
			return false;
		}

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

		$dp = ceil(($end - $start) / $outinterval);
		// $dpratio = $outinterval / $meta->interval;
		if ($dp < 1) {
			return false;
		}

        $rrd_options = array(
            "AVERAGE",
            "--start", $start,
            "--end", $end,
            "--resolution", $outinterval);
        if(!$output = rrd_fetch($this->dir.$id.".rrd", $rrd_options)) {
            $this->log->warn("PHPRRD:could not fetch data from feed id=$id");
            return false;
        }
        $data = array();

        foreach($output['data']['feed'] as $time => &$value) {
            if (!is_nan($value))
                $data[] = array($time * 1000, $value);
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

		if(!$output = rrd_lastupdate($this->dir . $id . ".rrd")) {
            $this->log->warn("PHPRRD:could not fetch data from feed id=$id");
            return false;
        }

		if ($output) {
			return array('time' => $output['last_update'], 'value' => $output['data'][0]);
		} else {
			return array('time' => 0, 'value' => 0);
		}
	}

	public function export($id, $start) {

	}

	public function delete($id) {
		if (!file_exists($this->dir . $id . ".rrd")) {
			return false;
		}

		unlink($this->dir . $id . ".rrd");
	}

	public function get_feed_size($id) {
		if (!file_exists($this->dir . $id . ".rrd")) {
			return false;
		}

		return (filesize($this->dir . $id . ".rrd"));
	}

	public function csv_export($id, $start, $end, $outinterval) {
		$id = intval($id);
		$start = intval($start);
		$end = intval($end);
		$outinterval = (int) $outinterval;

		$dp = ceil(($end - $start) / $outinterval);

		// $dpratio = $outinterval / $meta->interval;
		if ($dp < 1) {
			return false;
		}

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

        $rrd_options = array(
            "AVERAGE",
            "--start", $start,
            "--end", $end,
            "--resolution", $outinterval);
        if(!$output = rrd_fetch($this->dir.$id.".rrd", $rrd_options)) {
            $this->log->warn("PHPRRD:could not fetch data from feed id=$id");
            return false;
        }
        $data = array();

        foreach($output['data']['feed'] as $time => &$value) {
            if (!is_nan($value))
                fwrite($exportfh, $time . "," . number_format($value, 2) . "\n");
        }
        fclose($exportfh);
		exit;
	}

}