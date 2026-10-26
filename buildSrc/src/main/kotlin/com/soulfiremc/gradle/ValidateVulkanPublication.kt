package com.soulfiremc.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault(because = "Validates the selected publication platforms without producing files")
abstract class ValidateVulkanPublication : DefaultTask() {
  @get:Input
  abstract val platforms: ListProperty<String>

  @TaskAction
  fun validate() {
    require(platforms.get().toSet() == VulkanPlatforms.all.toSet()) {
      "Publish all six Vulkan runtimes together with -PvulkanPlatforms=all -PvulkanRuntimeDirectory=<prebuilt-directory>."
    }
  }
}
