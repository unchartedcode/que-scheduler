source 'https://rubygems.org'

group :development, :test do
  gem 'rake'
  gem 'sequel',          :require => nil
  gem 'activerecord',    :require => nil
  gem 'pg',              :require => nil, :platform => :ruby
  gem 'minitest-reporters', require: nil
  gem 'minitest-line', require: nil
  gem 'byebug'
end

gem 'que-data', git: 'https://github.com/unchartedcode/que-data.git', branch: '8625524726ad11b24bb737696244f35b598a4edf', require: false

# Specify your gem's dependencies in que-scheduler.gemspec
gemspec
