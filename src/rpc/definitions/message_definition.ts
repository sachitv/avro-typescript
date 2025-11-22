import { createType, type CreateTypeOptions } from "../../type/create_type.ts";
import { NullType } from "../../schemas/primitive/null_type.ts";
import type { RecordType } from "../../schemas/complex/record_type.ts";
import type { UnionType } from "../../schemas/complex/union_type.ts";
import type { Type } from "../../schemas/type.ts";
import type { MessageDefinition } from "./protocol_definitions.ts";

/**
 * Schema-aware representation of an Avro RPC message definition including
 * request, response, and error types.
 */
export class Message {
  /** The name of the message. */
  readonly name: string;
  /** Optional documentation for the message. */
  readonly doc?: string;
  /** The record type representing the request parameters. */
  readonly requestType: RecordType;
  /** The type returned by the message (if not one-way). */
  readonly responseType: Type;
  /** The union type of possible errors. */
  readonly errorType: UnionType;
  /** Whether the message is one-way (no response). */
  readonly oneWay: boolean;

  /**
   * Creates a new Message definition.
   * @param name The name of the message.
   * @param attrs The message attributes from the protocol definition.
   * @param opts Options for creating types (namespace, registry).
   */
  constructor(
    name: string,
    attrs: MessageDefinition,
    opts: CreateTypeOptions,
  ) {
    this.name = name;
    this.doc = attrs.doc;
    this.requestType = createType({
      name,
      type: "request",
      fields: attrs.request,
    }, opts) as RecordType;

    if (!attrs.response) {
      throw new Error("missing response");
    }
    this.responseType = createType(attrs.response, opts);

    const errors = attrs.errors ? attrs.errors.slice() : [];
    errors.unshift("string");
    this.errorType = createType(errors, opts) as UnionType;

    this.oneWay = !!attrs["one-way"];
    if (this.oneWay) {
      if (!(this.responseType instanceof NullType)) {
        throw new Error("one-way messages must return 'null'");
      }
      if (this.errorType.getTypes().length > 1) {
        throw new Error("one-way messages cannot declare errors");
      }
    }
  }

  /**
   * Converts the message definition back to a JSON-compatible object.
   */
  toJSON(): Record<string, unknown> {
    const json: Record<string, unknown> = {
      request: extractRequestFields(this.requestType),
      response: this.responseType.toJSON(),
    };
    if (this.doc) {
      json.doc = this.doc;
    }
    if (this.oneWay) {
      json["one-way"] = true;
    }
    const errors = this.errorType.toJSON();
    if (Array.isArray(errors) && errors.length > 1) {
      json.errors = errors.slice(1);
    }
    return json;
  }
}

function extractRequestFields(requestType: RecordType): unknown[] {
  const json = requestType.toJSON() as Record<string, unknown>;
  return json.fields as unknown[];
}
