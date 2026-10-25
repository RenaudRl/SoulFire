package com.soulfiremc.gradle

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class GenerateLauncherAssetsTest {
  @TempDir
  lateinit var directory: Path

  private fun jar(name: String, entries: Map<String, ByteArray>): File {
    val file = directory.resolve(name).toFile()
    ZipOutputStream(file.outputStream()).use { output ->
      entries.forEach { (path, content) ->
        output.putNextEntry(ZipEntry(path))
        output.write(content)
        output.closeEntry()
      }
    }
    return file
  }

  @Test
  fun `nested libraries replace identical runtime artifacts without removing other versions or transitives`() {
    val bridge = jar("bridge.jar", mapOf("bridge.bin" to byteArrayOf(1, 2, 3)))
    val renamedBridge = directory.resolve("resolved-bridge.jar").toFile().also { bridge.copyTo(it) }
    val otherVersion = jar("bridge-new.jar", mapOf("bridge.bin" to byteArrayOf(4, 5, 6)))
    val transitive = jar("dependency.jar", mapOf("dependency.bin" to byteArrayOf(7, 8, 9)))
    val mod = jar("mod.jar", mapOf(
      "fabric.mod.json" to """{"jars":[{"file":"META-INF/jars/bridge.jar"}]}""".toByteArray(),
      "META-INF/jars/bridge.jar" to bridge.readBytes()
    ))

    val runtime = GenerateLauncherAssets.runtimeLibraries(
      setOf(mod, bridge, renamedBridge, otherVersion, transitive),
      mapOf(mod.name to "com.soulfiremc:mod:1.0")
    )

    assertEquals(listOf(mod, otherVersion, transitive), runtime)
  }

  @Test
  fun `only declared nested jars replace runtime artifacts`() {
    val dependency = jar("dependency.jar", mapOf("dependency.bin" to byteArrayOf(1, 2, 3)))
    val mod = jar("mod.jar", mapOf(
      "fabric.mod.json" to "{}".toByteArray(),
      "META-INF/jars/dependency.jar" to dependency.readBytes()
    ))
    val files = setOf(mod, dependency)

    assertEquals(files.toList(), GenerateLauncherAssets.runtimeLibraries(files, mapOf(mod.name to "com.soulfiremc:mod:1.0")))
  }
}
