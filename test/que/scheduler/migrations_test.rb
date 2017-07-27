require 'test_helper'

describe Que::Scheduler::Migrations do
  it "should be able to perform migrations up and down" do
    # Migration #1 creates the table with a priority default of 1, migration
    # #2 ups that to 100.

    default = proc do
      result = Que.execute <<-SQL
        select adsrc::boolean
        from pg_attribute a
        join pg_class c on c.oid = a.attrelid
        join pg_attrdef on adrelid = attrelid AND adnum = attnum
        where relname = 'que_scheduler'
        and attname = 'enabled'
      SQL

      result.first[:adsrc]
    end

    default.call.must_equal false

    # Clean up.
    Que.migrate!
  end
end
