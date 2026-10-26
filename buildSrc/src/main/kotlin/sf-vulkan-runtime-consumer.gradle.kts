plugins {
  java
}

val vulkanRuntimeDependencies = configurations.dependencyScope("vulkanRuntimeDependencies")

// Keep platform selection on execution classpaths. A Maven dependency here would
// select the metadata-only JAR rather than the required platform classifiers.
sourceSets.configureEach {
  configurations.named(runtimeClasspathConfigurationName) {
    extendsFrom(vulkanRuntimeDependencies.get())
  }
}

dependencies {
  add(vulkanRuntimeDependencies.name, project(":vulkan-runtime"))
}
