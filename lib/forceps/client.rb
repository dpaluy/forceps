module Forceps
  class Client
    attr_reader :options

    def configure(options={})
      @options = options.merge(default_options)
      @models_without_table = []
      @models_with_table = []

      declare_remote_model_classes
      make_associations_reference_remote_classes

      logger.debug "Classes handled by Forceps: #{@models_with_table.collect(&:name).inspect}"
    end

    private

    def logger
      Forceps.logger
    end

    def default_options
      {}
    end

    def model_classes
      @model_classes ||= filtered_model_classes
    end

    def filtered_model_classes
      (ActiveRecord::Base.descendants - model_classes_to_exclude).reject do |klass|
        klass.name.start_with?('HABTM_')
      end
    end

    def model_classes_to_exclude
      if Rails::VERSION::MAJOR >= 4
        [ActiveRecord::SchemaMigration]
      else
        []
      end
    end

    def declare_remote_model_classes
      return if @remote_classes_defined
      model_classes.each { |remote_class| declare_remote_model_class(remote_class) }
      @remote_classes_defined = true
    end

    def declare_remote_model_class(klass)
      full_class_name = klass.name
      head = Forceps::Remote

      path = full_class_name.split("::")
      class_name = path.pop

      path.each do |module_name|
        if head.const_defined?(module_name, false)
          head = head.const_get(module_name, false)
        else
          head = head.const_set(module_name, Module.new)
        end
      end

      instance = build_new_remote_class(klass)
      return unless instance

      head.const_set(class_name, instance)

      remote_class_for(full_class_name).establish_connection :remote
    end

    def build_new_remote_class(local_class)
      return if local_class.name == 'PurchaseDetail'

      needs_type_condition = (local_class.base_class != ActiveRecord::Base)

      begin
        needs_type_condition &&= local_class.finder_needs_type_condition?
      rescue ActiveRecord::StatementInvalid => e
        raise e unless e.cause.is_a?(PG::UndefinedTable)

        # Prevent error caused by auto-generated framework models.
        puts "Ignoring model without table: #{local_class}"
        @models_without_table << local_class
        return nil
      end

      Class.new(local_class) do
        self.table_name = local_class.table_name

        include Forceps::ActsAsCopyableModel

        # Intercept instantiation of records to make the 'type' column point to the corresponding remote class
        if Rails::VERSION::MAJOR >= 4
          def self.instantiate(record, column_types = {})
            __make_sti_column_point_to_forceps_remote_class(record)
            super
          end
        else
          def self.instantiate(record)
            __make_sti_column_point_to_forceps_remote_class(record)
            super
          end
        end

        def self.sti_name
          name.gsub("Forceps::Remote::", "")
        end

        def self.__make_sti_column_point_to_forceps_remote_class(record)
          if record[inheritance_column].present?
            record[inheritance_column] = "Forceps::Remote::#{record[inheritance_column]}"
          end
        end

        # We don't want to include STI condition automatically (the base class extends the original one)
        unless needs_type_condition
          def self.finder_needs_type_condition?
            false
          end
        end
      end
    end

    def remote_class_for(full_class_name)
      head = Forceps::Remote
      full_class_name.split("::").each do |mod|
        head = head.const_get(mod)
      end
      head
    end

    def make_associations_reference_remote_classes
      @models_with_table = model_classes - @models_without_table
      @models_with_table.each do |model_class|
        make_associations_reference_remote_classes_for(model_class)
      end
    end

    def make_associations_reference_remote_classes_for(model_class)
      model_class._reflections.values.each do |association|
        next if association.class_name =~ /Forceps::Remote/ || association.class_name =~ /HABTM/ rescue next
        reference_remote_class(model_class, association)
      end
    end

    def reference_remote_class(model_class, association)
      remote_model_class = remote_class_for(model_class.name)

      if association.options[:polymorphic]
        reference_remote_class_in_polymorphic_association(association, remote_model_class)
      else
        reference_remote_class_in_normal_association(association, remote_model_class)
      end
    end

    def reference_remote_class_in_polymorphic_association(association, remote_model_class)
      foreign_type_attribute_name = association.foreign_type

      remote_model_class.send(:define_method, association.foreign_type) do
        "Forceps::Remote::#{super()}"
      end

      remote_model_class.send(:define_method, "[]") do |attribute_name|
        if (attribute_name.to_s == foreign_type_attribute_name)
          "Forceps::Remote::#{super(attribute_name)}"
        else
          super(attribute_name)
        end
      end
    end

    def reference_remote_class_in_normal_association(association, remote_model_class)
      related_remote_class = remote_class_for(association.klass.name)

      cloned_association = association.dup
      cloned_association.instance_variable_set("@klass", related_remote_class)
      ActiveRecord::Reflection.add_reflection(remote_model_class, cloned_association.name, cloned_association)
    end
  end
end
