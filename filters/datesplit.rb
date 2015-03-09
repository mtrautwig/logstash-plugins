# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# The datesplit filter expands the @timestamp field into its individual
# components, like day of week and hour. This allows for data analysis
# tasks like finding the busiest day, or the time when you best run
# updates.
class LogStash::Filters::DateSplit < LogStash::Filters::Base

  config_name "datesplit"
  milestone 1
  
  # Specify a time zone canonical ID to be used for date parsing.
  # The valid IDs are listed on the [Joda.org available time zones page](http://joda-time.sourceforge.net/timezones.html).
  # If this is not specified the platform default will be used.
  # Canonical ID is good as it takes care of daylight saving time for you
  # For example, `America/Los_Angeles` or `Europe/France` are valid IDs.
  config :timezone, :validate => :string

  # Specify a locale to be used for date formatting. If this is not specified,
  # English will be used
  #
  # The locale is mostly necessary to be set for formatting month names and
  # weekday names.
  #
  config :locale, :validate => :string, :default => "en"
  
  # field from where the timestamp is taken. If not provided,
  # default to using the @timestamp field of the event.
  config :source, :validate => :string, :default => "@timestamp"
  
  private
  def parseLocale(localeString)
    return nil if localeString == nil
    matches = localeString.match(/(?<lang>.+?)(?:_(?<country>.+?))?(?:_(?<variant>.+))?/)
    lang = matches['lang'] == nil ? "" : matches['lang'].strip()
    country = matches['country'] == nil ? "" : matches['country'].strip()
    variant = matches['variant'] == nil ? "" : matches['variant'].strip()
    return lang.length > 0 ? java.util.Locale.new(lang, country, variant) : nil
  end
  
  public
  def register
    require "java"
    @locale = parseLocale(@config["locale"][0]) if @config["locale"] != nil and @config["locale"][0] != nil
  end # def register
  
  public
  def filter(event)
    return unless filter?(event)
  
    if event[@source] && event[@source].is_a?(Time)
      time = org.joda.time.DateTime.new(event[@source].to_i * 1000)
      if @timezone
        time = time.withZone(org.joda.time.DateTimeZone.forID(@timezone))
      else
        time = time.withZone(org.joda.time.DateTimeZone.getDefault())
      end
      
      event["time_year"] = time.year().get()
      #event["time_quarter"]
      #event["time_week_of_year"]
      event["time_month"] = time.monthOfYear().get()
      event["time_month_name"] = time.monthOfYear().getAsText(@locale)
      event["time_day_of_month"] = time.dayOfMonth().get()
      event["time_weekday"] = time.dayOfWeek().get()
      event["time_weekday_name"] = time.dayOfWeek().getAsText(@locale)
      event["time_hour"] = time.hourOfDay().get()
    end
      
  end # def filter
  
end
