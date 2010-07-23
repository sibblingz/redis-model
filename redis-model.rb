class RedisModel

  class << self 
    attr_reader :persistent_attribute_names
    attr_reader :unsendable_attribute_names
    attr_reader :forcesendable_attribute_names
    attr_reader :callbacks
    attr_reader :associations
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
        self.class_eval "
          def #{attribute}; @#{attribute}; end
          def #{attribute}=(value); @#{attribute}=#{value_conversion}; end
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
    hsh.each{ |key, value| self.send "#{key}=", value }
    self.class.defaults.each{ |key, value| self.send "#{key}=", value if self.send( key ).nil? }
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
    REDIS.sadd self.class.class_set_redis_key, self.id
  end
  
  def remove_from_class_set
    REDIS.srem self.class.class_set_redis_key, self.id
  end
  
  def update_attributes( attributes_hash )
    attributes_hash.each{ |key, value| puts "#{key}=#{value}" }
    attributes_hash.each{ |key, value| self.send "#{key}=", value }
    save
  end
  
  def attributes
    hsh = {}
    self.class.persistent_attribute_names.each{|attr_name| hsh[attr_name] = self.send attr_name}
    hsh
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
    self.class.persistent_attribute_names.each{ |attribute| 
      value = self.send(attribute)
      REDIS.hset(redis_key, attribute, value) unless value.nil?
    }
    
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
  end
  
  def self.find( id )
    hsh = REDIS.hgetall( redis_key(id) )
    raise RecordNotFound.new unless hsh.has_key? 'id'

    obj = self.new
    # weed out any attributes that don't exist on the model, like if an attribute was removed from the class but still exist in the database
    for attribute, value in hsh
      if @persistent_attribute_names.include? attribute.to_sym
        obj.send("#{attribute}=", value)
      else
        #Rails.logger.info "Tried to set attribute '#{attribute}' = '#{value}' on #{self.to_s}, but it doesn't have that attribute (perhaps the model definition changed recently?)"
      end
    end
    obj.id = id.to_i
    obj
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
  end
  
  def self.destroy( ids )
    ids.each{ |id| self.find(id).andand.destroy }
  end
  
  def is_dependent_destroy?( association )
    association[:args].andand[:dependent] == :destroy
  end
  
  def destroy_association( association )
    children = self.send(association[:name])

    if(children.is_a? Array)
      children.each{|elem| elem.destroy }
    else
      children.destroy
    end
    
    REDIS.del self.set_redis_key(association[:name])
  end
  
  def destroy_dependents
    return if self.class.associations.nil?
    self.class.associations.each do |association|
      destroy_association(association) if is_dependent_destroy?(association)
    end
  end
  
  def self.count
    REDIS.scard class_set_redis_key
  end
  
  def self.all
    id_list = REDIS.smembers class_set_redis_key
    id_list.map{|id| self.find(id)}
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
    
    # def garments
    define_method list_name do
      klass = self.klass_for_association( list_name, args )
      key = set_redis_key(list_name)
      REDIS.smembers( key ).map{ |item_id| klass.find(item_id) }
    end
    
    # def add_garment
    define_method "add_#{list_name.to_s.singularize}" do |new_item|
      key = set_redis_key(list_name)
      REDIS.sadd( key, new_item.id )
    end
    
    # def remove_garment
    define_method "remove_#{list_name.to_s.singularize}" do |item|
      key = set_redis_key(list_name)
      REDIS.srem( key, item.id )
    end
    
    # def garment_ids
    define_method "#{list_name.to_s.singularize}_ids" do
      REDIS.smembers( set_redis_key(list_name) )
    end
    
  end
  
  def self.belongs_to(parent_name, args={})
    register_association( parent_name, args )
    
    define_method parent_name do
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
    @temporary_forcesendables.push property_name
  end
  
  def json_sendable_value( value )
    if value.is_a? Time
      return value.to_i
    end
    return value
  end
  
  def to_json(options={})
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
  
  # Writes content of this table to db/table_name.yml, or the specified file.
  def self.dump_to_file( path = nil, force = false )
    path ||= yaml_file_path
    if !File.exist?(path) || force
      write_file(File.expand_path( path , RAILS_ROOT), self.all.to_yaml)
    else
      # show_diff( path )
      # puts "Are you sure? [y/N]"
      # input = gets
      # if ( input == 'y' )
        write_file(File.expand_path( path , RAILS_ROOT), self.all.to_yaml)
      # end
    end
  end
  
  def self.update_from_records(records)
    all_ids = self.all.collect{ |model| model.id.to_i }
    new_ids = []
    
    records.each do |record|   
      begin
        puts "record = #{record.inspect}"
        existing_model = self.find( record.id )
        existing_model = record 
        existing_model.save
      
        all_ids.delete( record.id.to_i )
      rescue RedisModel::RecordNotFound
        new_model = self.new
        new_model = record
        new_ids.push( record.id )
        new_model.save

        if self.name == 'StorePossession'
          new_model.store.add_store_possession(new_model)
        end
      end
    end
    
    self.destroy( all_ids )
    
    new_ids.each{ |new_id| REDIS.sadd(class_set_redis_key, new_id) }
    
    {:deleted=>all_ids, :created=>new_ids}
  end
  
  def self.update_from_file( path=nil )    
    path = yaml_file_path if path.nil?
    records = YAML::load( File.open( File.expand_path(path, RAILS_ROOT) ) )
    update_from_records(records)
  end
  
  
  def self.register_association( association_name, args={} )
    @associations ||= []
    @associations.push({:name => association_name, :args => args}) unless associations.include? association_name
  end
end
