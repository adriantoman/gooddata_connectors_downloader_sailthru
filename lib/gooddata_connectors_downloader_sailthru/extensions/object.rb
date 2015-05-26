# encoding: utf-8

class Object
  class << self
    def class_from_string(class_name)
      fail ArgumentError if class_name.nil? || class_name.empty?
      fail ArgumentError unless class_name.is_a?(String)

      class_name.split('::').inject(Object) { |a, e| a.const_get e }
    end
  end
end
