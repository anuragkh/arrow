/**
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef subscription_service_H
#define subscription_service_H

#include <thrift/TDispatchProcessor.h>
#include <thrift/async/TConcurrentClientSyncInfo.h>
#include "subscription_service_types.h"

namespace jiffy { namespace storage {

#ifdef _MSC_VER
  #pragma warning( push )
  #pragma warning (disable : 4250 ) //inheriting methods via dominance 
#endif

class subscription_serviceIf {
 public:
  virtual ~subscription_serviceIf() {}
  virtual void notification(const std::string& op, const std::string& data) = 0;
  virtual void control(const response_type type, const std::vector<std::string> & ops, const std::string& error) = 0;
};

class subscription_serviceIfFactory {
 public:
  typedef subscription_serviceIf Handler;

  virtual ~subscription_serviceIfFactory() {}

  virtual subscription_serviceIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) = 0;
  virtual void releaseHandler(subscription_serviceIf* /* handler */) = 0;
};

class subscription_serviceIfSingletonFactory : virtual public subscription_serviceIfFactory {
 public:
  subscription_serviceIfSingletonFactory(const ::apache::thrift::stdcxx::shared_ptr<subscription_serviceIf>& iface) : iface_(iface) {}
  virtual ~subscription_serviceIfSingletonFactory() {}

  virtual subscription_serviceIf* getHandler(const ::apache::thrift::TConnectionInfo&) {
    return iface_.get();
  }
  virtual void releaseHandler(subscription_serviceIf* /* handler */) {}

 protected:
  ::apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> iface_;
};

class subscription_serviceNull : virtual public subscription_serviceIf {
 public:
  virtual ~subscription_serviceNull() {}
  void notification(const std::string& /* op */, const std::string& /* data */) {
    return;
  }
  void control(const response_type /* type */, const std::vector<std::string> & /* ops */, const std::string& /* error */) {
    return;
  }
};

typedef struct _subscription_service_notification_args__isset {
  _subscription_service_notification_args__isset() : op(false), data(false) {}
  bool op :1;
  bool data :1;
} _subscription_service_notification_args__isset;

class subscription_service_notification_args {
 public:

  subscription_service_notification_args(const subscription_service_notification_args&);
  subscription_service_notification_args& operator=(const subscription_service_notification_args&);
  subscription_service_notification_args() : op(), data() {
  }

  virtual ~subscription_service_notification_args() throw();
  std::string op;
  std::string data;

  _subscription_service_notification_args__isset __isset;

  void __set_op(const std::string& val);

  void __set_data(const std::string& val);

  bool operator == (const subscription_service_notification_args & rhs) const
  {
    if (!(op == rhs.op))
      return false;
    if (!(data == rhs.data))
      return false;
    return true;
  }
  bool operator != (const subscription_service_notification_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const subscription_service_notification_args & ) const;

  template <class Protocol_>
  uint32_t read(Protocol_* iprot);
  template <class Protocol_>
  uint32_t write(Protocol_* oprot) const;

};


class subscription_service_notification_pargs {
 public:


  virtual ~subscription_service_notification_pargs() throw();
  const std::string* op;
  const std::string* data;

  template <class Protocol_>
  uint32_t write(Protocol_* oprot) const;

};

typedef struct _subscription_service_control_args__isset {
  _subscription_service_control_args__isset() : type(false), ops(false), error(false) {}
  bool type :1;
  bool ops :1;
  bool error :1;
} _subscription_service_control_args__isset;

class subscription_service_control_args {
 public:

  subscription_service_control_args(const subscription_service_control_args&);
  subscription_service_control_args& operator=(const subscription_service_control_args&);
  subscription_service_control_args() : type((response_type)0), error() {
  }

  virtual ~subscription_service_control_args() throw();
  response_type type;
  std::vector<std::string>  ops;
  std::string error;

  _subscription_service_control_args__isset __isset;

  void __set_type(const response_type val);

  void __set_ops(const std::vector<std::string> & val);

  void __set_error(const std::string& val);

  bool operator == (const subscription_service_control_args & rhs) const
  {
    if (!(type == rhs.type))
      return false;
    if (!(ops == rhs.ops))
      return false;
    if (!(error == rhs.error))
      return false;
    return true;
  }
  bool operator != (const subscription_service_control_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const subscription_service_control_args & ) const;

  template <class Protocol_>
  uint32_t read(Protocol_* iprot);
  template <class Protocol_>
  uint32_t write(Protocol_* oprot) const;

};


class subscription_service_control_pargs {
 public:


  virtual ~subscription_service_control_pargs() throw();
  const response_type* type;
  const std::vector<std::string> * ops;
  const std::string* error;

  template <class Protocol_>
  uint32_t write(Protocol_* oprot) const;

};

template <class Protocol_>
class subscription_serviceClientT : virtual public subscription_serviceIf {
 public:
  subscription_serviceClientT(apache::thrift::stdcxx::shared_ptr< Protocol_> prot) {
    setProtocolT(prot);
  }
  subscription_serviceClientT(apache::thrift::stdcxx::shared_ptr< Protocol_> iprot, apache::thrift::stdcxx::shared_ptr< Protocol_> oprot) {
    setProtocolT(iprot,oprot);
  }
 private:
  void setProtocolT(apache::thrift::stdcxx::shared_ptr< Protocol_> prot) {
  setProtocolT(prot,prot);
  }
  void setProtocolT(apache::thrift::stdcxx::shared_ptr< Protocol_> iprot, apache::thrift::stdcxx::shared_ptr< Protocol_> oprot) {
    piprot_=iprot;
    poprot_=oprot;
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
 public:
  apache::thrift::stdcxx::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return this->piprot_;
  }
  apache::thrift::stdcxx::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return this->poprot_;
  }
  void notification(const std::string& op, const std::string& data);
  void send_notification(const std::string& op, const std::string& data);
  void control(const response_type type, const std::vector<std::string> & ops, const std::string& error);
  void send_control(const response_type type, const std::vector<std::string> & ops, const std::string& error);
 protected:
  apache::thrift::stdcxx::shared_ptr< Protocol_> piprot_;
  apache::thrift::stdcxx::shared_ptr< Protocol_> poprot_;
  Protocol_* iprot_;
  Protocol_* oprot_;
};

typedef subscription_serviceClientT< ::apache::thrift::protocol::TProtocol> subscription_serviceClient;

template <class Protocol_>
class subscription_serviceProcessorT : public ::apache::thrift::TDispatchProcessorT<Protocol_> {
 protected:
  ::apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> iface_;
  virtual bool dispatchCall(::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, const std::string& fname, int32_t seqid, void* callContext);
  virtual bool dispatchCallTemplated(Protocol_* iprot, Protocol_* oprot, const std::string& fname, int32_t seqid, void* callContext);
 private:
  typedef  void (subscription_serviceProcessorT::*ProcessFunction)(int32_t, ::apache::thrift::protocol::TProtocol*, ::apache::thrift::protocol::TProtocol*, void*);
  typedef void (subscription_serviceProcessorT::*SpecializedProcessFunction)(int32_t, Protocol_*, Protocol_*, void*);
  struct ProcessFunctions {
    ProcessFunction generic;
    SpecializedProcessFunction specialized;
    ProcessFunctions(ProcessFunction g, SpecializedProcessFunction s) :
      generic(g),
      specialized(s) {}
    ProcessFunctions() : generic(NULL), specialized(NULL) {}
  };
  typedef std::map<std::string, ProcessFunctions> ProcessMap;
  ProcessMap processMap_;
  void process_notification(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_notification(int32_t seqid, Protocol_* iprot, Protocol_* oprot, void* callContext);
  void process_control(int32_t seqid, ::apache::thrift::protocol::TProtocol* iprot, ::apache::thrift::protocol::TProtocol* oprot, void* callContext);
  void process_control(int32_t seqid, Protocol_* iprot, Protocol_* oprot, void* callContext);
 public:
  subscription_serviceProcessorT(::apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> iface) :
    iface_(iface) {
    processMap_["notification"] = ProcessFunctions(
      &subscription_serviceProcessorT::process_notification,
      &subscription_serviceProcessorT::process_notification);
    processMap_["control"] = ProcessFunctions(
      &subscription_serviceProcessorT::process_control,
      &subscription_serviceProcessorT::process_control);
  }

  virtual ~subscription_serviceProcessorT() {}
};

typedef subscription_serviceProcessorT< ::apache::thrift::protocol::TDummyProtocol > subscription_serviceProcessor;

template <class Protocol_>
class subscription_serviceProcessorFactoryT : public ::apache::thrift::TProcessorFactory {
 public:
  subscription_serviceProcessorFactoryT(const ::apache::thrift::stdcxx::shared_ptr< subscription_serviceIfFactory >& handlerFactory) :
      handlerFactory_(handlerFactory) {}

  ::apache::thrift::stdcxx::shared_ptr< ::apache::thrift::TProcessor > getProcessor(const ::apache::thrift::TConnectionInfo& connInfo);

 protected:
  ::apache::thrift::stdcxx::shared_ptr< subscription_serviceIfFactory > handlerFactory_;
};

typedef subscription_serviceProcessorFactoryT< ::apache::thrift::protocol::TDummyProtocol > subscription_serviceProcessorFactory;

class subscription_serviceMultiface : virtual public subscription_serviceIf {
 public:
  subscription_serviceMultiface(std::vector<apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> >& ifaces) : ifaces_(ifaces) {
  }
  virtual ~subscription_serviceMultiface() {}
 protected:
  std::vector<apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> > ifaces_;
  subscription_serviceMultiface() {}
  void add(::apache::thrift::stdcxx::shared_ptr<subscription_serviceIf> iface) {
    ifaces_.push_back(iface);
  }
 public:
  void notification(const std::string& op, const std::string& data) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->notification(op, data);
    }
    ifaces_[i]->notification(op, data);
  }

  void control(const response_type type, const std::vector<std::string> & ops, const std::string& error) {
    size_t sz = ifaces_.size();
    size_t i = 0;
    for (; i < (sz - 1); ++i) {
      ifaces_[i]->control(type, ops, error);
    }
    ifaces_[i]->control(type, ops, error);
  }

};

// The 'concurrent' client is a thread safe client that correctly handles
// out of order responses.  It is slower than the regular client, so should
// only be used when you need to share a connection among multiple threads
template <class Protocol_>
class subscription_serviceConcurrentClientT : virtual public subscription_serviceIf {
 public:
  subscription_serviceConcurrentClientT(apache::thrift::stdcxx::shared_ptr< Protocol_> prot) {
    setProtocolT(prot);
  }
  subscription_serviceConcurrentClientT(apache::thrift::stdcxx::shared_ptr< Protocol_> iprot, apache::thrift::stdcxx::shared_ptr< Protocol_> oprot) {
    setProtocolT(iprot,oprot);
  }
 private:
  void setProtocolT(apache::thrift::stdcxx::shared_ptr< Protocol_> prot) {
  setProtocolT(prot,prot);
  }
  void setProtocolT(apache::thrift::stdcxx::shared_ptr< Protocol_> iprot, apache::thrift::stdcxx::shared_ptr< Protocol_> oprot) {
    piprot_=iprot;
    poprot_=oprot;
    iprot_ = iprot.get();
    oprot_ = oprot.get();
  }
 public:
  apache::thrift::stdcxx::shared_ptr< ::apache::thrift::protocol::TProtocol> getInputProtocol() {
    return this->piprot_;
  }
  apache::thrift::stdcxx::shared_ptr< ::apache::thrift::protocol::TProtocol> getOutputProtocol() {
    return this->poprot_;
  }
  void notification(const std::string& op, const std::string& data);
  void send_notification(const std::string& op, const std::string& data);
  void control(const response_type type, const std::vector<std::string> & ops, const std::string& error);
  void send_control(const response_type type, const std::vector<std::string> & ops, const std::string& error);
 protected:
  apache::thrift::stdcxx::shared_ptr< Protocol_> piprot_;
  apache::thrift::stdcxx::shared_ptr< Protocol_> poprot_;
  Protocol_* iprot_;
  Protocol_* oprot_;
  ::apache::thrift::async::TConcurrentClientSyncInfo sync_;
};

typedef subscription_serviceConcurrentClientT< ::apache::thrift::protocol::TProtocol> subscription_serviceConcurrentClient;

#ifdef _MSC_VER
  #pragma warning( pop )
#endif

}} // namespace

#include "subscription_service.tcc"
#include "subscription_service_types.tcc"

#endif
