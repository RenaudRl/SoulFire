/*
 * SoulFire
 * Copyright (C) 2026  AlexProgrammerDE
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.soulfiremc.server.api;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import com.soulfiremc.grpc.generated.ApiDocsProto;
import com.soulfiremc.server.command.CommandSourceStack;
import com.soulfiremc.server.settings.lib.SettingsObject;
import com.soulfiremc.server.settings.lib.SettingsPageRegistry;
import com.soulfiremc.server.settings.property.BooleanProperty;
import io.grpc.BindableService;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/// Owns the plugin API registration phase and the immutable runtime catalog.
public final class PluginApiRegistry {
  private static final Pattern PLUGIN_ID = Pattern.compile("[a-z0-9]+(?:-[a-z0-9]+)*");
  private static final Pattern PERMISSION_NAME = Pattern.compile("[a-z][a-z0-9_]*");
  private static final Pattern PAGE_ID = Pattern.compile("[a-z0-9]+(?:-[a-z0-9]+)*");
  private static final Pattern COMMAND_NAME = Pattern.compile("[a-z0-9]+(?:-[a-z0-9]+)*");
  private static final Pattern PACKAGE_NAME =
    Pattern.compile("soulfire\\.plugin\\.([a-z][a-z0-9_]*)\\.v([1-9][0-9]*)");

  private final Map<String, PluginState> plugins = new LinkedHashMap<>();
  private final Set<Consumer<Snapshot>> listeners = new CopyOnWriteArraySet<>();
  private final Set<Consumer<PublishedPluginEvent>> eventListeners = new CopyOnWriteArraySet<>();
  private long revision;
  private long eventSequence;

  synchronized PluginContext createContext(PluginInfo pluginInfo) {
    validatePluginInfo(pluginInfo);
    if (plugins.putIfAbsent(pluginInfo.id(), new PluginState(pluginInfo)) != null) {
      throw new IllegalStateException("Duplicate plugin ID: " + pluginInfo.id());
    }
    changed();
    return new PluginContext(
      pluginInfo,
      new PluginRpcRegistry(this, pluginInfo),
      new PluginPermissionRegistry(this, pluginInfo),
      new BotTaskProviderRegistry(this, pluginInfo),
      new PluginEventRegistry(this, pluginInfo),
      new PluginSettingsRegistry(this, pluginInfo),
      new PluginCommandRegistry(this, pluginInfo),
      new PluginSdkMetadataRegistry(this, pluginInfo)
    );
  }

  synchronized void discard(String pluginId) {
    if (plugins.remove(pluginId) != null) {
      changed();
    }
  }

  synchronized RegisteredPluginPermission registerPermission(
    PluginInfo owner,
    PluginPermission permission
  ) {
    var state = state(owner);
    if (!PERMISSION_NAME.matcher(permission.name()).matches()) {
      throw new IllegalArgumentException(
        "Plugin permission names must match %s: %s".formatted(PERMISSION_NAME, permission.name())
      );
    }
    var id = "plugin.%s.%s".formatted(owner.id(), permission.name());
    var registered = new RegisteredPluginPermission(id, owner.id(), permission);
    if (state.permissions.putIfAbsent(id, registered) != null) {
      throw new IllegalStateException("Duplicate plugin permission ID: " + id);
    }
    changed();
    return registered;
  }

  synchronized void registerSdkMetadata(PluginInfo owner, PluginSdkMetadata metadata) {
    var state = state(owner);
    if (state.metadataRegistered) {
      throw new IllegalStateException("SDK metadata already registered for plugin: " + owner.id());
    }
    state.sdkMetadata = metadata;
    state.metadataRegistered = true;
    changed();
  }

  synchronized PluginRpcRegistration registerRpc(PluginInfo owner, BindableService bindableService) {
    var state = state(owner);
    var service = Objects.requireNonNull(bindableService, "bindableService").bindService();
    var grpcDescriptor = service.getServiceDescriptor();
    if (!(grpcDescriptor.getSchemaDescriptor() instanceof ProtoFileDescriptorSupplier supplier)) {
      throw new IllegalArgumentException(
        "Plugin RPC service must expose protobuf descriptors: " + grpcDescriptor.getName()
      );
    }

    var fileDescriptor = supplier.getFileDescriptor();
    var descriptor = fileDescriptor.getServices().stream()
      .filter(candidate -> candidate.getFullName().equals(grpcDescriptor.getName()))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException(
        "Service descriptor not found in protobuf file: " + grpcDescriptor.getName()
      ));
    validateServiceNamespace(owner, state.sdkMetadata, descriptor);
    if (descriptor.getMethods().stream().anyMatch(Descriptors.MethodDescriptor::isClientStreaming)) {
      throw new IllegalArgumentException(
        "Plugin RPC service uses unsupported client streaming: " + descriptor.getFullName()
      );
    }
    if (allServices().stream()
      .anyMatch(existing -> existing.descriptor().getFullName().equals(descriptor.getFullName()))) {
      throw new IllegalStateException("Duplicate plugin RPC service: " + descriptor.getFullName());
    }

    var methodPermissions = validateMethodPermissions(state, descriptor);
    var registration = new PluginRpcRegistration(owner, service, descriptor, methodPermissions);
    state.services.add(registration);
    changed();
    return registration;
  }

  synchronized <I extends Message, R extends Message>
  BotTaskProviderRegistration<I, R> registerTaskProvider(
    PluginInfo owner,
    PluginBotTaskProvider<I, R> provider,
    List<RegisteredPluginPermission> permissions
  ) {
    var state = state(owner);
    var prototype = Objects.requireNonNull(provider, "provider").inputPrototype();
    var resultPrototype = Objects.requireNonNull(
      provider.resultPrototype(),
      "provider.resultPrototype()"
    );
    var progressPrototype = Objects.requireNonNull(
      provider.progressPrototype(),
      "provider.progressPrototype()"
    );
    var descriptor = prototype.getDescriptorForType();
    var packageName = descriptor.getFile().getPackage();
    validateTypeNamespace(owner, state.sdkMetadata, packageName);
    validateTypeNamespace(
      owner,
      state.sdkMetadata,
      resultPrototype.getDescriptorForType().getFile().getPackage()
    );
    progressPrototype.ifPresent(progress ->
      validateTypeNamespace(
        owner,
        state.sdkMetadata,
        progress.getDescriptorForType().getFile().getPackage()
      )
    );
    var typeUrl = "type.googleapis.com/" + descriptor.getFullName();
    var resultTypeUrl =
      "type.googleapis.com/" + resultPrototype.getDescriptorForType().getFullName();
    var progressTypeUrl = progressPrototype.map(progress ->
      "type.googleapis.com/" + progress.getDescriptorForType().getFullName()
    );
    if (allTaskProviders().stream().anyMatch(existing -> existing.typeUrl().equals(typeUrl))) {
      throw new IllegalStateException("Duplicate plugin task request type: " + typeUrl);
    }
    if (permissions.isEmpty()) {
      throw new IllegalArgumentException(
        "Every plugin task provider must declare at least one permission: " + typeUrl
      );
    }
    var registeredPermissions = permissions.stream()
      .map(permission -> {
        var registered = state.permissions.get(permission.id());
        if (registered == null || !registered.equals(permission)) {
          throw new IllegalArgumentException(
            "Task provider references an unregistered permission: " + permission.id()
          );
        }
        return registered;
      })
      .distinct()
      .toList();
    var registration = new BotTaskProviderRegistration<>(
      owner,
      typeUrl,
      resultTypeUrl,
      progressTypeUrl,
      provider,
      registeredPermissions
    );
    state.tasks.add(registration);
    changed();
    return registration;
  }

  synchronized <E extends Message> PluginEventRegistration<E> registerEvent(
    PluginInfo owner,
    E prototype,
    List<RegisteredPluginPermission> permissions
  ) {
    var state = state(owner);
    var eventPrototype = Objects.requireNonNull(prototype, "prototype");
    validateTypeNamespace(
      owner,
      state.sdkMetadata,
      eventPrototype.getDescriptorForType().getFile().getPackage()
    );
    var typeUrl = "type.googleapis.com/" + eventPrototype.getDescriptorForType().getFullName();
    if (allEventTypes().stream().anyMatch(existing -> existing.typeUrl().equals(typeUrl))) {
      throw new IllegalStateException("Duplicate plugin event type: " + typeUrl);
    }
    if (permissions.isEmpty()) {
      throw new IllegalArgumentException(
        "Every plugin event type must declare at least one permission: " + typeUrl
      );
    }
    var registeredPermissions = validateRegisteredPermissions(state, permissions, "Event type " + typeUrl);
    var registration = new PluginEventRegistration<>(
      this,
      owner,
      typeUrl,
      eventPrototype,
      registeredPermissions
    );
    state.events.add(registration);
    changed();
    return registration;
  }

  synchronized List<PluginEventRegistration<?>> eventTypes(String pluginId) {
    return List.copyOf(requiredState(pluginId).events);
  }

  synchronized PluginSettingsPageRegistration registerSettingsPage(
    PluginInfo owner,
    PluginSettingsPageRegistration.Scope scope,
    Class<? extends SettingsObject> settingsClass,
    String id,
    String pageName,
    String iconId,
    @Nullable BooleanProperty<?> enabledProperty
  ) {
    var state = state(owner);
    Objects.requireNonNull(scope, "scope");
    Objects.requireNonNull(settingsClass, "settingsClass");
    validateText("settings page ID", id, PAGE_ID);
    requireNonBlank("settings page name", pageName);
    requireNonBlank("settings page icon", iconId);
    if (plugins.values().stream()
      .flatMap(plugin -> plugin.settingsPages.stream())
      .anyMatch(page -> page.scope() == scope && page.id().equals(id))) {
      throw new IllegalStateException(
        "Duplicate %s settings page ID: %s".formatted(scope.name().toLowerCase(Locale.ROOT), id)
      );
    }
    var registration = new PluginSettingsPageRegistration(
      owner,
      scope,
      settingsClass,
      id,
      pageName,
      iconId,
      enabledProperty
    );
    state.settingsPages.add(registration);
    return registration;
  }

  synchronized List<PluginSettingsPageRegistration> settingsPages(String pluginId) {
    return List.copyOf(requiredState(pluginId).settingsPages);
  }

  public synchronized void applySettingsPages(
    PluginSettingsPageRegistration.Scope scope,
    SettingsPageRegistry target
  ) {
    plugins.values().stream()
      .flatMap(plugin -> plugin.settingsPages.stream())
      .filter(page -> page.scope() == scope)
      .forEach(page -> target.addPluginPage(
        page.settingsClass(),
        page.id(),
        page.pageName(),
        page.owner(),
        page.iconId(),
        page.enabledProperty()
      ));
  }

  synchronized PluginCommandRegistration registerCommand(
    PluginInfo owner,
    LiteralArgumentBuilder<CommandSourceStack> command
  ) {
    var state = state(owner);
    var root = Objects.requireNonNull(command, "command");
    validateText("command name", root.getLiteral(), COMMAND_NAME);
    if (plugins.values().stream()
      .flatMap(plugin -> plugin.commands.stream())
      .anyMatch(existing -> existing.name().equals(root.getLiteral()))) {
      throw new IllegalStateException("Duplicate plugin command: " + root.getLiteral());
    }
    var registration = new PluginCommandRegistration(owner, root);
    state.commands.add(registration);
    return registration;
  }

  synchronized List<PluginCommandRegistration> commands(String pluginId) {
    return List.copyOf(requiredState(pluginId).commands);
  }

  public synchronized void applyCommands(
    CommandDispatcher<CommandSourceStack> dispatcher
  ) {
    plugins.values().stream()
      .flatMap(plugin -> plugin.commands.stream())
      .forEach(command -> {
        if (dispatcher.getRoot().getChild(command.name()) != null) {
          throw new IllegalStateException(
            "Plugin command conflicts with an existing command: " + command.name()
          );
        }
        dispatcher.register(command.command());
      });
  }

  public synchronized long currentEventSequence() {
    return eventSequence;
  }

  public synchronized EventSubscription subscribeEvents(Consumer<PublishedPluginEvent> listener) {
    eventListeners.add(listener);
    return new EventSubscription(eventSequence, () -> eventListeners.remove(listener));
  }

  synchronized <E extends Message> long publishEvent(
    PluginEventRegistration<E> registration,
    PluginEventTarget target,
    E payload
  ) {
    var state = requiredState(registration.owner().id());
    if (!state.events.contains(registration)) {
      throw new IllegalStateException("Plugin event type is not registered: " + registration.typeUrl());
    }
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(payload, "payload");
    if (payload.getDescriptorForType() != registration.prototype().getDescriptorForType()) {
      throw new IllegalArgumentException(
        "Plugin event payload type does not match " + registration.typeUrl()
      );
    }
    registration.permissions().forEach(permission ->
      validateEventTarget(permission, target, registration.typeUrl()));
    var event = new PublishedPluginEvent(
      ++eventSequence,
      registration,
      target,
      payload,
      System.currentTimeMillis()
    );
    eventListeners.forEach(listener -> listener.accept(event));
    return event.sequence();
  }

  public synchronized List<PluginRpcRegistration> rpcServices() {
    return List.copyOf(allServices());
  }

  synchronized List<PluginRpcRegistration> rpcServices(String pluginId) {
    return List.copyOf(requiredState(pluginId).services);
  }

  synchronized List<RegisteredPluginPermission> permissions(String pluginId) {
    return List.copyOf(requiredState(pluginId).permissions.values());
  }

  synchronized List<BotTaskProviderRegistration<?, ?>> taskProviders(String pluginId) {
    return List.copyOf(requiredState(pluginId).tasks);
  }

  private static List<RegisteredPluginPermission> validateRegisteredPermissions(
    PluginState state,
    List<RegisteredPluginPermission> permissions,
    String ownerDescription
  ) {
    return permissions.stream()
      .map(permission -> {
        var registered = state.permissions.get(permission.id());
        if (registered == null || !registered.equals(permission)) {
          throw new IllegalArgumentException(
            ownerDescription + " references an unregistered permission: " + permission.id()
          );
        }
        return registered;
      })
      .distinct()
      .toList();
  }

  private static void validateEventTarget(
    RegisteredPluginPermission permission,
    PluginEventTarget target,
    String typeUrl
  ) {
    switch (permission.definition().scope()) {
      case GLOBAL -> {
        return;
      }
      case INSTANCE -> requireTarget(target.instanceId(), "instance", typeUrl);
      case BOT -> {
        requireTarget(target.instanceId(), "instance", typeUrl);
        requireTarget(target.botId(), "bot", typeUrl);
      }
      case TASK -> {
        requireTarget(target.instanceId(), "instance", typeUrl);
        requireTarget(target.taskId(), "task", typeUrl);
      }
    }
  }

  private static void requireTarget(Optional<UUID> value, String kind, String typeUrl) {
    if (value.isEmpty()) {
      throw new IllegalArgumentException(
        "Plugin event %s requires a %s target".formatted(typeUrl, kind)
      );
    }
  }

  public synchronized List<BotTaskProviderRegistration<?, ?>> taskProviders() {
    return List.copyOf(allTaskProviders());
  }

  public synchronized List<PluginApiDefinition> catalog() {
    return plugins.values().stream()
      .filter(state -> !state.services.isEmpty() || !state.tasks.isEmpty() || !state.events.isEmpty())
      .map(PluginApiRegistry::definition)
      .toList();
  }

  public synchronized Optional<PluginApiDefinition> find(String pluginId) {
    var state = plugins.get(pluginId);
    if (state == null || (state.services.isEmpty() && state.tasks.isEmpty() && state.events.isEmpty())) {
      return Optional.empty();
    }
    return Optional.of(definition(state));
  }

  /// Finds an active dynamic permission by its globally unique ID.
  public synchronized Optional<RegisteredPluginPermission> findPermission(String permissionId) {
    return plugins.values().stream()
      .map(state -> state.permissions.get(permissionId))
      .filter(Objects::nonNull)
      .findFirst();
  }

  public synchronized long revision() {
    return revision;
  }

  public AutoCloseable subscribe(Consumer<Snapshot> listener) {
    listeners.add(listener);
    return () -> listeners.remove(listener);
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(revision, catalog());
  }

  private static PluginApiDefinition definition(PluginState state) {
    var descriptorSet = descriptorSet(state.services, state.tasks, state.events);
    return new PluginApiDefinition(
      state.pluginInfo,
      state.sdkMetadata,
      List.copyOf(state.permissions.values()),
      List.copyOf(state.services),
      List.copyOf(state.tasks),
      List.copyOf(state.events),
      descriptorSet,
      sha256(descriptorSet.toByteArray())
    );
  }

  private static DescriptorProtos.FileDescriptorSet descriptorSet(
    List<PluginRpcRegistration> registrations,
    List<BotTaskProviderRegistration<?, ?>> taskProviders,
    List<PluginEventRegistration<?>> eventTypes
  ) {
    var descriptors = new TreeMap<String, Descriptors.FileDescriptor>();
    registrations.forEach(registration ->
      collectFiles(registration.descriptor().getFile(), descriptors));
    taskProviders.forEach(registration ->
      collectFiles(
        registration.provider().inputPrototype().getDescriptorForType().getFile(),
        descriptors
      ));
    taskProviders.forEach(registration ->
      collectFiles(
        registration.provider().resultPrototype().getDescriptorForType().getFile(),
        descriptors
      ));
    taskProviders.forEach(registration ->
      registration.provider().progressPrototype().ifPresent(progress ->
        collectFiles(progress.getDescriptorForType().getFile(), descriptors)
      ));
    eventTypes.forEach(registration ->
      collectFiles(
        registration.prototype().getDescriptorForType().getFile(),
        descriptors
      ));
    var ordered = new LinkedHashMap<String, Descriptors.FileDescriptor>();
    descriptors.values().forEach(descriptor -> collectFiles(descriptor, ordered));
    return DescriptorProtos.FileDescriptorSet.newBuilder()
      .addAllFile(ordered.values().stream().map(PluginApiRegistry::descriptorProto).toList())
      .build();
  }

  // Generated Java descriptors may omit computed JSON names from their compact protos.
  // Include them for consumers that load descriptor sets without recomputing metadata.
  private static DescriptorProtos.FileDescriptorProto descriptorProto(Descriptors.FileDescriptor descriptor) {
    var builder = descriptor.toProto().toBuilder();
    descriptor.getMessageTypes().forEach(message -> builder.setMessageType(message.getIndex(), descriptorProto(message)));
    descriptor.getExtensions().forEach(field -> builder.setExtension(field.getIndex(), descriptorProto(field)));
    return builder.build();
  }

  private static DescriptorProtos.DescriptorProto descriptorProto(Descriptors.Descriptor descriptor) {
    var builder = descriptor.toProto().toBuilder();
    descriptor.getFields().forEach(field -> builder.setField(field.getIndex(), descriptorProto(field)));
    descriptor.getNestedTypes().forEach(message -> builder.setNestedType(message.getIndex(), descriptorProto(message)));
    descriptor.getExtensions().forEach(field -> builder.setExtension(field.getIndex(), descriptorProto(field)));
    return builder.build();
  }

  private static DescriptorProtos.FieldDescriptorProto descriptorProto(Descriptors.FieldDescriptor descriptor) {
    return descriptor.toProto().toBuilder().setJsonName(descriptor.getJsonName()).build();
  }

  private static void collectFiles(
    Descriptors.FileDescriptor descriptor,
    Map<String, Descriptors.FileDescriptor> descriptors
  ) {
    if (descriptors.containsKey(descriptor.getName())) {
      return;
    }
    descriptor.getDependencies().forEach(dependency -> collectFiles(dependency, descriptors));
    descriptors.put(descriptor.getName(), descriptor);
  }

  private static byte[] sha256(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("SHA-256 must be available", e);
    }
  }

  private Map<String, List<RegisteredPluginPermission>> validateMethodPermissions(
    PluginState state,
    Descriptors.ServiceDescriptor service
  ) {
    var result = new LinkedHashMap<String, List<RegisteredPluginPermission>>();
    for (var method : service.getMethods()) {
      var options = method.getOptions();
      var permissionIds = options.hasExtension(ApiDocsProto.apiMethod)
        ? options.getExtension(ApiDocsProto.apiMethod).getPermissionsList()
        : List.<String>of();
      if (permissionIds.isEmpty()) {
        throw new IllegalArgumentException(
          "Every plugin RPC must declare at least one permission: %s/%s"
            .formatted(service.getFullName(), method.getName())
        );
      }
      var permissions = permissionIds.stream()
        .map(id -> {
          var permission = state.permissions.get(id);
          if (permission == null) {
            throw new IllegalArgumentException(
              "RPC method references an unregistered permission: " + id
            );
          }
          return permission;
        })
        .distinct()
        .toList();
      var docs = options.getExtension(ApiDocsProto.apiMethod);
      if (docs.getExposeToMcp()) {
        if (method.isClientStreaming() || method.isServerStreaming()) {
          throw new IllegalArgumentException(
            "Only unary plugin RPCs can be exposed to MCP: %s/%s"
              .formatted(service.getFullName(), method.getName())
          );
        }
        var requiresConfirmation = permissions.stream()
          .map(RegisteredPluginPermission::definition)
          .map(PluginPermission::risk)
          .anyMatch(risk ->
            risk == PluginPermission.Risk.MUTATION
              || risk == PluginPermission.Risk.DESTRUCTIVE
          );
        if (requiresConfirmation && !docs.getMcpRequiresConfirmation()) {
          throw new IllegalArgumentException(
            "Mutation and destructive plugin MCP tools must require confirmation: %s/%s"
              .formatted(service.getFullName(), method.getName())
          );
        }
      }
      result.put(method.getName(), permissions);
    }
    return Map.copyOf(result);
  }

  private static void validateServiceNamespace(
    PluginInfo owner,
    PluginSdkMetadata metadata,
    Descriptors.ServiceDescriptor descriptor
  ) {
    var packageName = descriptor.getFile().getPackage();
    validateTypeNamespace(owner, metadata, packageName);
  }

  private static void validateTypeNamespace(
    PluginInfo owner,
    PluginSdkMetadata metadata,
    String packageName
  ) {
    var matcher = PACKAGE_NAME.matcher(packageName);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
        "Plugin protobuf package must match soulfire.plugin.<plugin-id>.v<major>: " + packageName
      );
    }
    var normalizedId = owner.id().replace('-', '_');
    if (!matcher.group(1).equals(normalizedId)) {
      throw new IllegalArgumentException(
        "Plugin protobuf package belongs to %s, expected %s".formatted(matcher.group(1), normalizedId)
      );
    }
    if (Integer.parseInt(matcher.group(2)) != metadata.apiMajorVersion()) {
      throw new IllegalArgumentException(
        "Plugin protobuf package major does not match registered SDK metadata"
      );
    }
  }

  private static void validatePluginInfo(PluginInfo pluginInfo) {
    Objects.requireNonNull(pluginInfo, "pluginInfo");
    if (!PLUGIN_ID.matcher(pluginInfo.id()).matches()) {
      throw new IllegalArgumentException(
        "Plugin IDs must match %s: %s".formatted(PLUGIN_ID, pluginInfo.id())
      );
    }
  }

  private static void validateText(
    String description,
    String value,
    Pattern pattern
  ) {
    requireNonBlank(description, value);
    if (!pattern.matcher(value).matches()) {
      throw new IllegalArgumentException(
        "%s must match %s: %s".formatted(description, pattern, value)
      );
    }
  }

  private static void requireNonBlank(String description, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(description + " must not be blank");
    }
  }

  private PluginState state(PluginInfo owner) {
    var state = requiredState(owner.id());
    if (state.pluginInfo != owner && !state.pluginInfo.equals(owner)) {
      throw new IllegalStateException("Plugin metadata changed during registration: " + owner.id());
    }
    return state;
  }

  private PluginState requiredState(String pluginId) {
    var state = plugins.get(pluginId);
    if (state == null) {
      throw new IllegalStateException("Plugin is not in its registration phase: " + pluginId);
    }
    return state;
  }

  private List<PluginRpcRegistration> allServices() {
    return plugins.values().stream().flatMap(state -> state.services.stream()).toList();
  }

  private List<BotTaskProviderRegistration<?, ?>> allTaskProviders() {
    return plugins.values().stream().flatMap(state -> state.tasks.stream()).toList();
  }

  private List<PluginEventRegistration<?>> allEventTypes() {
    return plugins.values().stream().flatMap(state -> state.events.stream()).toList();
  }

  private void changed() {
    revision++;
    if (listeners.isEmpty()) {
      return;
    }
    var snapshot = snapshot();
    listeners.forEach(listener -> listener.accept(snapshot));
  }

  public record Snapshot(long revision, List<PluginApiDefinition> plugins) {
    public Snapshot {
      plugins = List.copyOf(plugins);
    }
  }

  public record EventSubscription(long sequence, AutoCloseable closeable) {}

  private static final class PluginState {
    private final PluginInfo pluginInfo;
    private final Map<String, RegisteredPluginPermission> permissions = new LinkedHashMap<>();
    private final List<PluginRpcRegistration> services = new ArrayList<>();
    private final List<BotTaskProviderRegistration<?, ?>> tasks = new ArrayList<>();
    private final List<PluginEventRegistration<?>> events = new ArrayList<>();
    private final List<PluginSettingsPageRegistration> settingsPages = new ArrayList<>();
    private final List<PluginCommandRegistration> commands = new ArrayList<>();
    private PluginSdkMetadata sdkMetadata = PluginSdkMetadata.experimental();
    private boolean metadataRegistered;

    private PluginState(PluginInfo pluginInfo) {
      this.pluginInfo = pluginInfo;
    }
  }
}
