require 'test_helper'
require 'parse-cron'
require 'byebug'

describe 'cron' do
  describe 'every minute' do
    [
      ["* * * * *",             "2011-08-15 12:00",  "2011-08-15 12:00",1],
      ["* * * * *",             "2011-08-15 02:25",  "2011-08-15 02:25",1],
      ["* * * * *",             "2011-08-15 02:59",  "2011-08-15 02:59",1],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'comma' do
    [
      ["* 3,6,9 * * *",         "2011-08-15 02:15",  "2011-08-15 03:00",1],
      ["30 3,6,9 * * *",        "2011-08-15 02:15",  "2011-08-15 03:30",1],
      ["* * * * 1,3",           "2010-04-15 10:15",  "2010-04-19 00:00",1],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'numbers' do
    [
      ["30 9 * * *",            "2011-08-15 10:15",  "2011-08-16 09:30",1],
      ["30 9 * * *",            "2011-08-31 10:15",  "2011-09-01 09:30",1],
      ["30 9 * * *",            "2011-09-30 10:15",  "2011-10-01 09:30",1],
      ["0 9 * * *",             "2011-12-31 10:15",  "2012-01-01 09:00",1],
      ["0 11 * * *",            "2011-12-31 10:15",  "2011-12-31 11:00",1],
      ["* * 12 * *",            "2010-04-15 10:15",  "2010-05-12 00:00",1],
      ["0 0 1 1 *",             "2010-04-15 10:15",  "2011-01-01 00:00",1],
      ["0 0 * * 1",             "2011-08-01 00:00",  "2011-08-01 00:00",1],
      ["0 0 * * 1",             "2011-07-25 00:00",  "2011-07-25 00:00",1],
      ["45 23 7 3 *",           "2011-01-01 00:00",  "2011-03-07 23:45",1],
      ["40 5 * * *",            "2014-02-01 15:56",  "2014-02-02 05:40",1],
      ["0 5 * * 1",             "2014-02-01 15:56",  "2014-02-03 05:00",1],
      ["10 8 15 * *",           "2014-02-01 15:56",  "2014-02-15 08:10",1],
      ["50 6 * * 1",            "2014-02-01 15:56",  "2014-02-03 06:50",1],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe '@' do
    [
      ["@yearly",               "2014-02-01 15:56",  "2015-01-01 00:00",1],
      ["@monthly",              "2014-02-01 15:56",  "2014-03-01 00:00",1],
      ["@weekly",               "2014-02-01 15:56",  "2014-02-02 00:00",1],
      ["@daily",                "2014-02-01 15:56",  "2014-02-02 00:00",1],
      ["@hourly",               "2014-02-01 15:56",  "2014-02-01 16:00",1],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'slash' do
    [
      ["*/15 * * * *",          "2011-08-15 02:02",  "2011-08-15 02:15",1],
      ["*/3 * * * *",           "2014-02-01 15:56",  "2014-02-01 15:57",1],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  describe 'comma/slash' do
    [
      # ["*/15,25 * * * *",       "2011-08-15 02:15",  "2011-08-15 02:25",1],
      # ["1-20/3 * * * *",        "2014-02-01 15:56",  "2014-02-01 16:01",1],
      # ["1-9/3,15-30/4 * * * *", "2014-02-01 15:56",  "2014-02-01 16:01",1],
      ["15-59/15 * * * *",      "2014-02-01 15:56",  "2014-02-01 16:15",1],
      ["15-59/15 * * * *",      "2014-02-01 15:00",  "2014-02-01 15:15",1],
      ["15-59/15 * * * *",      "2014-02-01 15:01",  "2014-02-01 15:15",1],
      ["15-59/15 * * * *",      "2014-02-01 15:16",  "2014-02-01 15:30",1],
      ["15-59/15 * * * *",      "2014-02-01 15:26",  "2014-02-01 15:30",1],
      ["15-59/15 * * * *",      "2014-02-01 15:36",  "2014-02-01 15:45",1],
      # ["15-59/15 * * * *",      "2014-02-01 15:45",  "2014-02-01 16:15",4],
      ["15-59/15 * * * *",      "2014-02-01 15:46",  "2014-02-01 16:15",3],
      ["15-59/15 * * * *",      "2014-02-01 15:46",  "2014-02-01 16:15",2],
    ].each do |line, now, expected_next, num|
      it "returns #{expected_next} for '#{line}' when now is #{now}" do
        assert_time(now, expected_next, line)
      end
    end
  end

  # describe 'range,range' do
  #   [
  #     # ["1-9,15-30 * * * *",     "2014-02-01 15:56",  "2014-02-01 16:01",1],
  #   ].each do |line, now, expected_next, num|
  #     it "returns #{expected_next} for '#{line}' when now is #{now}" do
  #       assert_time(now, expected_next, line)
  #     end
  #   end
  # end

private

  def assert_time(now, expected_next, line)
    parsed_now = Time.parse(now)
    expected = Time.parse(expected_next)

    result = DB.select{que_scheduler_parse_cron(line, parsed_now)}.first
    result[:que_scheduler_parse_cron].must_be_close_to expected, 0.1
  end
end
