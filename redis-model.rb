# Copyright (c) 2009-2010, Sibblingz Inc.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

require 'set'

class RedisModel

  class << self 
    attr_reader :persistent_attribute_names
    attr_reader :unsendable_attribute_names
    attr_reader :forcesendable_attribute_names
    attr_reader :callbacks
    attr_reader :associations
  end
  
  attr_accessor :old_attributes
  
  # WCH: Lifted straight out of ActiveRecord
  # Returns true if the +comparison_object+ is the same object, or is of the same type and has the same id.
  def ==(comparison_object)
    comparison_object.equal?(self) || (comparison_object.instance_of?(self.class) && comparison_object.id == id)
  end
  def eql?(comparison_object)
    self == (comparison_object)
  end
  def <=>(comparison_object)
    self.id <=> comparison_object.id
  end
  
  def to_boolean(value, nil_value = false)
    value.downcase! if value.class == String
    case value
    when "no","false",false, "0", 0
      false
    when "yes","true",true, "1", 1
      true
    when nil
      nil_value 
    else
      !!value
    end
  end
  
  def self.index_attributes(*args)
    args.each do |arg|
      self.class_eval "
        def self.find_by_#{arg}(name); #{self.name}.all.select {|g| g.#{arg} == name}[0]; end
      "
    end
  end
  
  def self.persistent_attributes(args = {})
    @persistent_attribute_names ||= []

    raise "do not list 'id' as a persistent attribute.  It is added by default" if args.has_key? :id
    args[:id] = :int

    for attribute, type in args
      if attribute == 'id'
        raise "do not list 'id' as a persistent attribute.  It is added by default" if args.include? :id
      else
        @persistent_attribute_names += [attribute]
        value_conversion = case type
        when :integer then "value.to_i"
        when :int then "value.to_i"
        when :float then "value.to_f"
        when :number then "value.to_f"
        when :string then "value.to_s"
        when :symbol then "value.to_sym"
        when :boolean then "to_boolean(value)"
        when :bool then "to_boolean(value)"
        when :datetime then "(value.blank?) ? nil : ( value.to_i == 0 ? Time.parse(value.to_s) : Time.at(value.to_i) )" # TODO: Implement a better datetime string conversion        else "value"
        end
        
        match_data = /^(.*)_id$/.match(attribute.to_s)
        set_function_line_two = match_data.nil? ? "" : "@#{match_data[1]} = nil"
        
        self.class_eval "
          def #{attribute}
            @#{attribute} 
          end
          
          def #{attribute}=(value) 
            @#{attribute}= #{value_conversion}
            #{set_function_line_two}
          end
        "
      end
    end
  end
  
  def self.create( attributes )
    new_record = self.new(attributes)
    new_record.save
    new_record
  end
  
  def clone_attributes
    temp = self.class.new(self.attributes)
    temp.generate_unique_id
    return temp
  end
  
  def self.defaults
    {}
  end
  
  def to_param
    id.to_s
  end
  
  def initialize(hsh={})
    update_attributes_without_save( self.class.defaults.stringify_keys.merge(hsh.stringify_keys) )
    
    # Let's try and just use update_attributes_without_save everywhere
    # hsh.each{ |key, value| self.send "#{key}=", value }
    # self.class.defaults.each{ |key, value| self.send "#{key}=", value if self.send( key ).nil? }
  end
  
  def self.redis_key( id )
    "#{self.name}_#{id}"
  end
  
  def self.class_set_redis_key
    "_all_#{self.name}_ids"
  end
  
  def redis_key
    self.class.redis_key( self.id )
  end
  
  def generate_unique_id
    self.id = REDIS.incr(self.class.name).to_i
  end
  
  def add_to_class_set
    raise "InvalidID" if self.id.to_i == 0
    REDIS.sadd self.class.class_set_redis_key, self.id
  end
  
  def remove_from_class_set
    REDIS.srem self.class.class_set_redis_key, self.id
  end
  
  def update_attributes_without_save( attributes_hash )
    persistent_attribute_names = self.class.persistent_attribute_names
    attributes_hash.each do |key, value|
      self.send( "#{key}=", value ) if persistent_attribute_names.include? key.to_sym
    end
  end
  
  def update_attributes( attributes_hash )
    update_attributes_without_save( attributes_hash )
    save
  end
  
  def attributes
    hsh = {}
    self.class.persistent_attribute_names.each{|attr_name| hsh[attr_name] = self.send attr_name}
    hsh
  end
  
  def old_attribute( attr_name )
    @old_attributes.nil? ? nil : @old_attributes[ attr_name ]
  end
  
  def save
    
    callbacks = self.class.callbacks
    
    
    # Before Save
    if callbacks && callbacks[:before_save]
      methods = callbacks[:before_save]
      methods.each{ |method_name| self.send method_name }
    end
    
    # Generate ID
    if self.id.blank?
      generate_unique_id 
      
      # Add id to list for class
      add_to_class_set 
    end
    
    # Save It To Redis
    updated_values = []
    to_delete = []
    #self.class.persistent_attribute_names.each{ |attribute| 
    self.class.persistent_attribute_names.each do |attribute|
      value = self.send(attribute)
      old_value = old_attribute(attribute)
      
      if value != old_value
        if value.nil?
          to_delete.push(attribute)
        else
          updated_values += [attribute, value]
        end
      end
    end
    
    if updated_values.size == 2
      REDIS.hset(redis_key, updated_values[0], updated_values[1])
    elsif updated_values.size > 2
      REDIS.hmset(redis_key, *updated_values)
    end
    to_delete.each{ |attr_name| REDIS.hdel(redis_key, attr_name) }
    self.old_attributes = self.attributes.dup
    
    # After Save
    if callbacks && callbacks[:after_save]
      methods = callbacks[:after_save]
      methods.each{ |method_name| self.send method_name }
    end

    true
  end
  
  def save!
    save
  end
  
  class RecordNotFound < Exception
    def initialize(klass, id)
      @klass = klass
      @id = id
    end
    
    def to_s
      "#{self.class.name}: #{@klass} #{@id}"
    end
  end
  
  def self.find_all_ids( ids )
    ids.map{|id| self.find(id, false)}.compact
  end
  
  class MissingRedisModelError < Exception
  end
  
  def self.find( id, whiny=true )
    hsh = REDIS.hgetall( redis_key(id) )
    if whiny
      raise RecordNotFound.new(self.name, id) unless hsh.has_key? 'id'
    else
      if !hsh.has_key?('id')
        if defined? NewRelic
          NewRelic::Agent.notice_error( MissingRedisModelError.new(), :custom_params => {:klass => self.name, :id => id} )
        end
        return nil
      end
    end

    obj = self.new( hsh )
    #     Let's try and just use update_attributes_without_save / new (which calls update_attributes_without_save)
    #     weed out any attributes that don't exist on the model, like if an attribute was removed from the class but still exist in the database
    #     for attribute, value in hsh
    #       if @persistent_attribute_names.include? attribute.to_sym
    #         obj.send("#{attribute}=", value)
    #       else
    #         #Rails.logger.info "Tried to set attribute '#{attribute}' = '#{value}' on #{self.to_s}, but it doesn't have that attribute (perhaps the model definition changed recently?)"
    #       end
    #     end
    obj.id = id.to_i
    obj.old_attributes = obj.attributes.dup
    obj
  end
  
  def modified?
    attributes.each do |k,v|
      return true if old_attributes[k] != v
    end
    false
  end
  
  def self.find_all( ids, strict=true )
    if strict
      return ids.map{|id| self.find(id)}
    else
      found_records = []
      ids.each do |id| 
        begin
          record = self.find(id)
          found_records.push record
        rescue RecordNotFound
        end
      end
      return found_records
    end
  end
  
  def destroy
    raise "not saved yet" if self.id.nil?

    callbacks = self.class.callbacks
        
    # Before Destroy
    if callbacks && callbacks[:before_destroy]
      methods = callbacks[:before_destroy]
      methods.each{ |method_name| self.send method_name }
    end

    REDIS.del redis_key
    remove_from_class_set
    destroy_dependents
    
    self.class.associations.andand.each do |association|
      begin
        parent = self.send(association[:name])
      rescue RecordNotFound
        return
      end
     
      removal_method = "remove_" + self.class.name.underscore
      if parent.respond_to?(removal_method)
        parent.send(removal_method, self)
      end
    end
    
    # After Destroy
    if callbacks && callbacks[:after_destroy]
      methods = callbacks[:after_destroy]
      methods.each{ |method_name| self.send method_name }
    end
  end
  
  def self.destroy( ids )
    [*ids].each{ |id| self.find(id).andand.destroy }
  end
  
  def is_dependent_destroy?( association )
    association[:args].andand[:dependent] == :destroy
  end
  
  def destroy_associated_models( association )
    children = self.send(association[:name])

    if(children.is_a? Array)
      children.each{|elem| elem.destroy }
    else
      children.destroy
    end
  end
  
  def destroy_dependents
    return if self.class.associations.nil?
    self.class.associations.each do |association|
      destroy_associated_models(association) if is_dependent_destroy?(association)
      
	  REDIS.del self.set_redis_key(association[:name])
    end
  end
  
  def self.count
    REDIS.scard class_set_redis_key
  end
  
  def self.all
    # self.all_ids.map{|id| self.find(id)}
    self.find_all_ids( self.all_ids )
  end
  
  def self.rand(args=nil)
    raise "Incorrect number of arguments passed for the redis_model. Did you mean Kernel.rand?" unless args.nil?
    id = REDIS.srandmember class_set_redis_key
    id.nil? ? nil : self.find(id)
  end
  
  def self.destroy_all
    while( true )
      elem = self.rand
      if elem.nil?
        break
      else
        elem.destroy
      end
    end
  end
  
  def klass_for_association( association_name, args )
    klass = nil
    if args[:polymorphic]
      klass = self.send("#{association_name}_type").constantize
    elsif args[:class_name]
      klass = args[:class_name].constantize
    else
      klass = association_name.to_s.singularize.classify.constantize
    end
  end
  
  def self.set_redis_key(model_id, list_name)
    "_list_#{self.name}_#{model_id}_#{list_name}"
  end
  
  def set_redis_key(list_name)
    self.class.set_redis_key(self.id, list_name)
  end
  
  # Associations
  def self.has_many(list_name, args={})
    register_association( list_name, args )
    
    if ( args[:through] )
      define_method list_name do
        self.send(args[:through]).map{ |item| item.send list_name.singularize }
      end
      
      define_method "add_#{list_name.to_s.singularize}" do |new_item|
        raise "cannot add via a through association"
      end
      
      return
    end
    
    # def items
    define_method list_name do
      klass = self.klass_for_association( list_name, args )
      key = set_redis_key(list_name)
      # REDIS.smembers( key ).map{ |item_id| klass.find(item_id) }
      klass.find_all_ids( REDIS.smembers(key) )
    end
    
    # def add_item
    define_method "add_#{list_name.to_s.singularize}" do |new_item|
      key = set_redis_key(list_name)
      REDIS.sadd( key, new_item.id )
    end
    
    # def remove_item
    define_method "remove_#{list_name.to_s.singularize}" do |item|
      key = set_redis_key(list_name)
      REDIS.srem( key, item.id )
    end
    
    # def item_ids
    define_method "#{list_name.to_s.singularize}_ids" do
      REDIS.smembers( set_redis_key(list_name) )
    end
    
  end
  
  def self.belongs_to(parent_name, args={})
    register_association( parent_name, args )
    
    self.class_eval "
      def #{parent_name}
        @#{parent_name} ||= calculate_#{parent_name}
        return @#{parent_name}
      end
    "
    
    define_method "calculate_#{parent_name}" do
      value = self.send "#{parent_name}_id"
      klass = self.klass_for_association( parent_name, args )
      begin
        klass.find value
      rescue RedisModel::RecordNotFound
        nil
      end
    end
  end
  
  def self.has_one(association_name, args={})
    register_association( association_name, args )
    
    define_method association_name do
      key = set_redis_key(association_name)
      klass = self.klass_for_association( association_name, args )
      klass.find( REDIS.smembers( key ).first )
    end
    
    define_method "#{association_name}=" do |new_item|
      key = set_redis_key( association_name )
      REDIS.del( key )
      REDIS.sadd( key, new_item.id )
    end
  end
  
  # Marshalling stuff
  def self.attr_unsendable( *args )
    @unsendable_attribute_names ||= []
    @unsendable_attribute_names += args
  end
  
  def self.attr_forcesendable( *args )
    @forcesendable_attribute_names ||= []
    @forcesendable_attribute_names += args
  end
  
  def logger
    RAILS_DEFAULT_LOGGER
  end
  
  def classname
    self.class.name
  end
  
  def temporarily_forcesend( property_name )
    @temporary_forcesendables ||= []
    @temporary_forcesendables.push property_name unless @temporary_forcesendables.include?(property_name)
  end
  
  def json_sendable_value( value )
    if value.is_a? Time
      return value.to_i
    end
    return value
  end
  
  def self.all_ids
    REDIS.smembers(self.class_set_redis_key).map{|x| x.to_i}.sort
  end
  
  def self.first
    min_id = self.first_id
    min_id ? self.find(min_id) : nil
  end
  
  def self.first_id
    self.all_ids.first
  end
  
  def self.last
    max_id = self.last_id
    max_id ? self.find(max_id) : nil
  end
  
  def self.last_id
    self.all_ids.last
  end
  
  def to_json(ignore_me={})
    hsh = {}
    associations = self.class.associations.andand.collect{|association| association[:name] } || []
    json_attribute_names = self.class.json_attribute_names + (@temporary_forcesendables || [])
    associations_to_send = json_attribute_names & associations
    other_stuff_to_send = json_attribute_names - associations
    other_stuff_to_send.each do |attribute_name| 
      value = self.send attribute_name
      sendable_value = json_sendable_value( value )
      hsh[attribute_name] = sendable_value
    end
    if associations_to_send.size > 0
      sub_hsh = {}
      associations_to_send.each do |association_to_send| 
        sub_hsh[association_to_send] = self.send association_to_send
      end
      hsh[:associations] = sub_hsh
    end
    ActiveSupport::JSON::encode(hsh)
  end
  
  def self.json_attribute_names
    return [:classname] + (@persistent_attribute_names || []) + (@forcesendable_attribute_names || []) - (@unsendable_attribute_names || [])
  end
  
  # Rails stuff that I wish I had
  
  # Errors
  def errors
    ActiveRecord::Errors.new( self )
  end
  
  # Callbacks
  def self.before_save( method_name )
    @callbacks ||= {}
    @callbacks[:before_save] ||= []
    @callbacks[:before_save].push method_name
  end
  
  def self.after_create( method_name )
    @callbacks ||= {}
    @callbacks[:after_create] ||= []
    @callbacks[:after_create].push method_name
  end

  def self.before_destroy( method_name )
    @callbacks ||= {}
    @callbacks[:before_destroy] ||= []
    @callbacks[:before_destroy].push method_name
  end
  
  def self.after_destroy( method_name )
    @callbacks ||= {}
    @callbacks[:after_destroy] ||= []
    @callbacks[:after_destroy].push method_name
  end
  
  # Validators
  def self.validates_presence_of( attribute_name )
    # Do something here
  end
  
  def valid?
    true
  end
  
  def self.validates_uniqueness_of( attribute_name, scope=nil )
    # Do something here
  end
  
  
  
  
  
  
  # Update From YAML
  def self.yaml_tag_class_name
    self.name
  end
  
  def self.write_file(path, content) # :nodoc:
    f = File.new(path, "w+")
    f.puts content
    f.close
  end
  
  def self.yaml_file_path
    "db/#{name.tableize.downcase}.yml"
  end

  def self.csv_file_path
    "db/#{name.tableize.downcase}.csv"
  end
  
  
  def yaml_attributes
    attributes.merge({:id => self.id})
  end
  
  # Writes content of this table to db/table_name.yml, or the specified file.
  def self.dump_to_yml( path = nil, force = false )
    path ||= yaml_file_path
    if !File.exist?(path) || force
      write_file(File.expand_path( path , RAILS_ROOT), self.all.map(&:yaml_attributes).to_yaml)
    else
        write_file(File.expand_path( path , RAILS_ROOT), self.all.map(&:yaml_attributes).to_yaml)
    end
  end
  
  def self.dump_to_csv( path = nil )
    path ||= csv_file_path
    write_file(File.expand_path( path, RAILS_ROOT), self.all_to_csv)
  end
  
  #  TODO: use FasterCSV for this (dump/load) and make sure to include the ID
  def self.all_to_csv
    all_records = self.all
    headers = all_records[0].attributes.keys.reject{|k| k == :id}
    csv = headers.join(',').to_s
    all_records.each do |record|
      csv << "\n"
      values = []
      headers.each do |v|
        values << record.send(v)
      end
      csv << values.join(',')
    end
    return csv
  end
  
  # def self.load_from_csv( path = nil )
  #   path ||= csv_file_path
  # end
  
  def self.update_from_records(records)
    all_ids = self.all_ids
    new_ids = []
    max_id = 1
        
    records.each do |record|   
      begin
        puts "record = #{record.inspect}"
        existing_model = self.find( record[:id] )
        existing_model.update_attributes( record )      
        all_ids.delete( record[:id].to_i )
      rescue RedisModel::RecordNotFound
        puts "record not found!"
        
        new_model = self.create(record)
        new_ids.push( record[:id] )

        max_id = [ max_id, record[:id].to_i ].max
        
      end
    end
    
    self.destroy( all_ids )
    new_ids.each{|id| REDIS.sadd self.class_set_redis_key, id}
    
    #Set the id dispatcher to the maximum we found
    REDIS.set self.class.name, max_id
    
    {:deleted=>all_ids, :created=>new_ids}
  end
  
  def self.update_from_yml( path=nil )    
    path = yaml_file_path if path.nil?
    records = YAML::load( File.open( File.expand_path(path, RAILS_ROOT) ) )
    update_from_records(records)
  end
  
  
  def self.register_association( association_name, args={} )
    @associations ||= []
    @associations.push({:name => association_name, :args => args}) unless associations.include? association_name
  end
  
  
  
  
  ###################
  # Migration Stuff #
  ###################
  
  def self.migrate_set(old_key, new_key)
    if REDIS.exists old_key
      set = REDIS.smembers old_key
      set.each{ |elem| REDIS.sadd new_key, elem }
      REDIS.del old_key
    end
    new_key
  end
  
  def self.migrate_set_with_expiry(old_key, new_key)
    if REDIS.exists old_key
      ttl = REDIS.ttl old_key
      self.migrate_set(old_key, new_key)
      REDIS.expireat new_key, (Time.now.to_i + ttl.to_i)
    end
    new_key
  end
  
  def self.migrate_hash(old_key, new_key)
    if REDIS.exists old_key
      hash = REDIS.hgetall old_key
      REDIS.hmset new_key, *hash.to_a.flatten
      REDIS.del old_key
    end
    new_key
  end
  
  def self.migrate_hash_with_expiry(old_key, new_key)
    if REDIS.exists old_key
      ttl = REDIS.ttl old_key
      self.migrate_hash(old_key, new_key)
      REDIS.expireat new_key, (Time.now.to_i + ttl.to_i)
    end
    new_key
  end
  
  def self.migrate_string(old_key, new_key)
    if REDIS.exists old_key
      string = REDIS.get old_key
      REDIS.set new_key, string
      REDIS.del old_key
    end
    new_key
  end
  
end
