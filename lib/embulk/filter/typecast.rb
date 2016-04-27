Embulk::JavaPlugin.register_filter(
  "typecast", "org.embulk.filter.typecast.TypecastFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
