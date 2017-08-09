require 'test_helper'
require 'byebug'

describe 'cron' do
  describe 'every minute' do
    [
      ["* * * * *",             "2011-08-15 12:00",  "2011-08-15 12:01"],
      ["* * * * *",             "2011-08-15 02:25",  "2011-08-15 02:26"],
      ["* * * * *",             "2011-08-15 02:59",  "2011-08-15 03:00"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'comma' do
    [
      ["* 3,6,9 * * *",         "2011-08-15 02:15",  "2011-08-15 03:00"],
      ["30 3,6,9 * * *",        "2011-08-15 02:15",  "2011-08-15 03:30"],
      ["* * * * 1,3",           "2010-04-15 10:15",  "2010-04-19 00:00"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'solid numbers' do
    [
      ["30 9 * * *",            "2011-08-15 10:15",  "2011-08-16 09:30"],
      ["30 9 * * *",            "2011-08-31 10:15",  "2011-09-01 09:30"],
      ["30 9 * * *",            "2011-09-30 10:15",  "2011-10-01 09:30"],
      ["0 9 * * *",             "2011-12-31 10:15",  "2012-01-01 09:00"],
      ["0 11 * * *",            "2011-12-31 10:15",  "2011-12-31 11:00"],
      ["* * 12 * *",            "2010-04-15 10:15",  "2010-05-12 00:00"],
      ["0 0 1 1 *",             "2010-04-15 10:15",  "2011-01-01 00:00"],
      ["0 0 * * 1",             "2011-08-01 00:00",  "2011-08-08 00:00"],
      ["0 0 * * 1",             "2011-07-25 00:00",  "2011-08-01 00:00"],
      ["45 23 7 3 *",           "2011-01-01 00:00",  "2011-03-07 23:45"],
      ["40 5 * * *",            "2014-02-01 15:56",  "2014-02-02 05:40"],
      ["0 5 * * 1",             "2014-02-01 15:56",  "2014-02-03 05:00"],
      ["10 8 15 * *",           "2014-02-01 15:56",  "2014-02-15 08:10"],
      ["50 6 * * 1",            "2014-02-01 15:56",  "2014-02-03 06:50"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'non standard' do
    [
      ["@yearly",               "2014-02-01 15:56",  "2015-01-01 00:00"],
      ["@monthly",              "2014-02-01 15:56",  "2014-03-01 00:00"],
      ["@weekly",               "2014-02-01 15:56",  "2014-02-02 00:00"],
      ["@daily",                "2014-02-01 15:56",  "2014-02-02 00:00"],
      ["@hourly",               "2014-02-01 15:56",  "2014-02-01 16:00"],
      ["* * * * MON,WED",       "2010-04-15 10:15",  "2010-04-19 00:00"],
      ["0 0 1 may,jul *",       "2013-05-14 15:00",  "2013-07-01 00:00"],
      ["0 0 1 MAY,JUL *",       "2013-05-14 15:00",  "2013-07-01 00:00"],
      ["1 2 * apr mOn",         "2014-02-01 15:56",  "2014-04-07 02:01"],
      ["1 2 3 jan sat",         "2014-02-01 15:56",  "2015-01-03 02:01"],
      ["1 2 3 4 thU",           "2014-02-01 15:56",  "2014-04-03 02:01"],
      ["1 2 2 jan 5",           "2014-02-01 15:56",  "2015-01-02 02:01"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'slash' do
    [
      ["*/15 * * * *",          "2011-08-15 02:02",  "2011-08-15 02:15"],
      ["*/3 * * * *",           "2014-02-01 15:56",  "2014-02-01 15:57"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'comma/slash' do
    [
      ["*/15,25 * * * *",       "2011-08-15 02:15",  "2011-08-15 02:25"],
      ["15-59/15 * * * *",      "2014-02-01 15:56",  "2014-02-01 16:15"],
      ["15-59/15 * * * *",      "2014-02-01 15:00",  "2014-02-01 15:15"],
      ["15-59/15 * * * *",      "2014-02-01 15:01",  "2014-02-01 15:15"],
      ["15-59/15 * * * *",      "2014-02-01 15:16",  "2014-02-01 15:30"],
      ["15-59/15 * * * *",      "2014-02-01 15:26",  "2014-02-01 15:30"],
      ["15-59/15 * * * *",      "2014-02-01 15:36",  "2014-02-01 15:45"],
      ["15-59/15 * * * *",      "2014-02-01 15:45",  "2014-02-01 16:15"],
      ["15-59/15 * * * *",      "2014-02-01 15:46",  "2014-02-01 16:15"],
      ["15-59/15 * * * *",      "2014-02-01 15:46",  "2014-02-01 16:15"],
      ["1-20/3 * * * *",        "2014-02-01 15:56",  "2014-02-01 16:01"],
      ["1-9/3,15-30/4 * * * *", "2014-02-01 15:56",  "2014-02-01 16:01"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'complex' do
    # Checks every minute for a single day to ensure it calculates the right cutoff times
    line = '0 0-3/3,10-22/3 * * *'
    now = Time.parse("2017-08-08 00:00:00")
    while now < Time.parse("2017-08-08 23:00:00")
      case now.hour
      when 0..2
        expected_next = "2017-08-08 03:00"
      when 3..9
        expected_next = "2017-08-08 10:00"
      when 10..12
        expected_next = "2017-08-08 13:00"
      when 13..15
        expected_next = "2017-08-08 16:00"
      when 16..18
        expected_next = "2017-08-08 19:00"
      when 19..21
        expected_next = "2017-08-08 22:00"
      when 22..23
        expected_next = "2017-08-09 00:00"
      end
      
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now.strftime('%F %H:%M:%S'), expected_next, line)
      end

      now += 60
    end
  end

  describe 'range,range' do
    [
      ["1-9,15-30 * * * *",     "2014-02-01 15:56",  "2014-02-01 16:01"],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

private

  def assert_time(now, expected_next, line)
    parsed_now = Time.parse(now)
    expected = Time.parse(expected_next)

    puts "Checking #{line}"
    result = DB.select{que_scheduler_parse_cron(line, parsed_now)}.first
    result[:que_scheduler_parse_cron].must_be_close_to expected, 0.1
  end
end
