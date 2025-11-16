import {
  createType,
  type CreateTypeOptions,
} from "../../../internal/createType/mod.ts";
import { NullType } from "../../../internal/schemas/null_type.ts";
import type { RecordType } from "../../../internal/schemas/record_type.ts";
import type { UnionType } from "../../../internal/schemas/union_type.ts";
import type { Type } from "../../../internal/schemas/type.ts";
import type { MessageDefinition } from "./protocol_definitions.ts";

export class Message {
  readonly name: string;
  readonly doc?: string;
  readonly requestType: RecordType;
  readonly responseType: Type;
  readonly errorType: UnionType;
  readonly oneWay: boolean;

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
